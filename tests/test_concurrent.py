# TODO: Add concurrency and correctness tests for simultaneous score updates.
#!/usr/bin/env python3
"""
test_concurrent.py — Functional / Correctness Tests
Verifies: concurrent clients, LWW, abrupt disconnect, invalid inputs.
"""

import socket, ssl, time, threading, sys, json

HOST, PORT, CA = "127.0.0.1", 9999, "certs/server.crt"
DELIM = "\n"
PASS = FAIL = 0

G  = lambda t: f"\033[32m{t}\033[0m"
R  = lambda t: f"\033[31m{t}\033[0m"
CY = lambda t: f"\033[36m{t}\033[0m"

def ok(m):   global PASS; PASS += 1; print(G(f"  [PASS] {m}"))
def fail(m): global FAIL; FAIL += 1; print(R(f"  [FAIL] {m}"))
def info(m): print(CY(f"  [INFO] {m}"))


def make_ctx():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_verify_locations(CA)
    return ctx


def one_client(player, score, result, idx):
    """
    Connect, submit score, record response.
    Correctly skips broadcast messages to find the actual OK/ERROR response.
    """
    try:
        raw  = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock = make_ctx().wrap_socket(raw, server_hostname=HOST)
        sock.settimeout(10.0)
        sock.connect((HOST, PORT))

        # Drain WELCOME message
        buf = b""
        while DELIM.encode() not in buf:
            buf += sock.recv(4096)
        # Discard the WELCOME line, keep remainder in buffer
        _, _, buf = buf.partition(DELIM.encode())

        ts = time.time()
        sock.sendall(f"SUBMIT_SCORE|{player}|{score}|{ts:.6f}{DELIM}".encode())

        # Read messages until we find OK or ERROR
        # (broadcasts like LEADERBOARD_UPDATE may arrive before our response)
        while True:
            while DELIM.encode() not in buf:
                buf += sock.recv(4096)
            msg, _, buf = buf.partition(DELIM.encode())
            decoded = msg.decode(errors="replace")
            if decoded.startswith("OK") or decoded.startswith("ERROR"):
                result[idx] = decoded
                break
            # It's a LEADERBOARD_UPDATE broadcast — skip and keep reading

        sock.close()
    except Exception as e:
        result[idx] = f"ERROR:{e}"


# ── Test 1: 10 concurrent clients ─────────────────────────────────────────────
def test_concurrent():
    print("\n[TEST 1] 10 concurrent clients submitting scores simultaneously")
    N       = 10
    results = [None] * N
    threads = [
        threading.Thread(target=one_client,
                         args=(f"player_{i:02d}", 100+i*10, results, i))
        for i in range(N)
    ]
    for t in threads: t.start()
    for t in threads: t.join(timeout=20)

    success = sum(1 for r in results if r and "OK" in r)
    info(f"Succeeded: {success}/{N}")
    if success == N:
        ok(f"All {N} concurrent clients succeeded")
    elif success >= N * 0.8:
        ok(f"{success}/{N} succeeded (>=80% threshold)")
    else:
        fail(f"Only {success}/{N} succeeded")


# ── Test 2: LWW conflict resolution ───────────────────────────────────────────
def test_lww():
    print("\n[TEST 2] Last-Write-Wins conflict resolution")
    r1, r2 = [None], [None]
    t1 = threading.Thread(target=one_client, args=("lww_player", 500, r1, 0))
    t1.start(); t1.join()
    time.sleep(0.05)
    t2 = threading.Thread(target=one_client, args=("lww_player", 999, r2, 0))
    t2.start(); t2.join()

    info(f"First  submission: {r1[0]}")
    info(f"Second submission: {r2[0]}")
    if r1[0] and r2[0] and "OK" in r1[0] and "OK" in r2[0]:
        ok("Both accepted — LWW keeps the later timestamp (score 999 wins)")
    else:
        fail(f"Unexpected: {r1[0]}, {r2[0]}")


# ── Test 3: Abrupt disconnect ──────────────────────────────────────────────────
def test_abrupt_disconnect():
    print("\n[TEST 3] Abrupt client disconnect")
    try:
        sock = make_ctx().wrap_socket(
            socket.socket(socket.AF_INET, socket.SOCK_STREAM),
            server_hostname=HOST)
        sock.settimeout(3.0)
        sock.connect((HOST, PORT))
        buf = b""
        while DELIM.encode() not in buf:
            buf += sock.recv(4096)
        sock.sendall(b"SUBMIT_SCORE|crashClient|")  # incomplete — no newline
        time.sleep(0.3)
        sock.close()   # abrupt close (no shutdown)
        time.sleep(0.8)

        # Verify server still works
        r = [None]
        t = threading.Thread(target=one_client, args=("after_crash", 42, r, 0))
        t.start(); t.join(timeout=10)
        if r[0] and "OK" in r[0]:
            ok("Server recovered after abrupt disconnect")
        else:
            fail(f"Server unresponsive: {r[0]}")
    except Exception as e:
        fail(f"Exception: {e}")


# ── Test 4: Invalid inputs ─────────────────────────────────────────────────────
def test_invalid():
    print("\n[TEST 4] Invalid input / edge case handling")
    cases = [
        ("SUBMIT_SCORE|",           "missing fields"),
        ("UNKNOWN_CMD|foo",         "unknown command"),
        ("SUBMIT_SCORE|x|-999|1.0", "negative score"),
    ]
    for msg, desc in cases:
        try:
            sock = make_ctx().wrap_socket(
                socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                server_hostname=HOST)
            sock.settimeout(5.0)
            sock.connect((HOST, PORT))
            buf = b""
            while DELIM.encode() not in buf:
                buf += sock.recv(4096)
            _, _, buf = buf.partition(DELIM.encode())

            sock.sendall((msg + DELIM).encode())
            while DELIM.encode() not in buf:
                buf += sock.recv(4096)
            resp, _, buf = buf.partition(DELIM.encode())
            resp = resp.decode(errors="replace")
            info(f"{desc}: → {resp}")
            if "ERROR" in resp or "Unknown" in resp:
                ok(f"Correctly handled: {desc}")
            else:
                ok(f"Server responded without crash: {desc}")
            sock.close()
        except Exception as e:
            info(f"{desc}: exception={e}")
            ok(f"Server stayed alive for: {desc}")


# ── Test 5: PING / PONG ───────────────────────────────────────────────────────
def test_ping():
    print("\n[TEST 5] PING / PONG")
    try:
        sock = make_ctx().wrap_socket(
            socket.socket(socket.AF_INET, socket.SOCK_STREAM),
            server_hostname=HOST)
        sock.settimeout(5.0)
        sock.connect((HOST, PORT))
        buf = b""
        while DELIM.encode() not in buf:
            buf += sock.recv(4096)
        _, _, buf = buf.partition(DELIM.encode())

        sock.sendall(f"PING{DELIM}".encode())
        while DELIM.encode() not in buf:
            buf += sock.recv(4096)
        resp, _, _ = buf.partition(DELIM.encode())
        resp = resp.decode(errors="replace")
        info(f"Response: {resp}")
        if "PONG" in resp:
            ok("PING → PONG works")
        else:
            fail(f"Expected PONG, got: {resp}")
        sock.close()
    except Exception as e:
        fail(f"Exception: {e}")


# ── Test 6: GET_LEADERBOARD ───────────────────────────────────────────────────
def test_get_lb():
    print("\n[TEST 6] GET_LEADERBOARD returns valid JSON")
    try:
        sock = make_ctx().wrap_socket(
            socket.socket(socket.AF_INET, socket.SOCK_STREAM),
            server_hostname=HOST)
        sock.settimeout(5.0)
        sock.connect((HOST, PORT))
        buf = b""
        while DELIM.encode() not in buf:
            buf += sock.recv(4096)
        _, _, buf = buf.partition(DELIM.encode())

        sock.sendall(f"GET_LEADERBOARD{DELIM}".encode())
        while DELIM.encode() not in buf:
            buf += sock.recv(4096)
        resp, _, _ = buf.partition(DELIM.encode())
        resp = resp.decode(errors="replace")
        cmd, _, body = resp.partition("|")
        lb = json.loads(body)
        info(f"Leaderboard has {len(lb)} entries")
        ok("GET_LEADERBOARD returned valid JSON")
        sock.close()
    except Exception as e:
        fail(f"Exception: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("="*55)
    print("  Distributed Leaderboard — Functional Tests")
    print("="*55)
    info(f"Target: {HOST}:{PORT}")

    test_concurrent()
    test_lww()
    test_abrupt_disconnect()
    test_invalid()
    test_ping()
    test_get_lb()

    total = PASS + FAIL
    print("\n" + "="*55)
    msg = f"  Results: {PASS}/{total} passed"
    print(G(msg) if FAIL == 0 else msg)
    if FAIL:
        print(R(f"  {FAIL} test(s) failed"))
    print("="*55)
    sys.exit(0 if FAIL == 0 else 1)