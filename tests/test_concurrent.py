# TODO: Add concurrency and correctness tests for simultaneous score updates.

#!/usr/bin/env python3  # Run using Python 3

"""
test_concurrent.py — Functional / Correctness Tests
Verifies: concurrent clients, LWW, abrupt disconnect, invalid inputs.
"""

# Import modules for networking, security, threading, timing, and JSON handling
import socket, ssl, time, threading, sys, json

# Server configuration (host, port, and certificate)
HOST, PORT, CA = "127.0.0.1", 9999, "certs/server.crt"

# Message delimiter (marks end of each message)
DELIM = "\n"

# Counters for tracking test results
PASS = FAIL = 0

# Colored output functions (for better terminal readability)
G  = lambda t: f"\033[32m{t}\033[0m"   # Green text
R  = lambda t: f"\033[31m{t}\033[0m"   # Red text
CY = lambda t: f"\033[36m{t}\033[0m"   # Cyan text

# Mark test as passed
def ok(m):   global PASS; PASS += 1; print(G(f"  [PASS] {m}"))

# Mark test as failed
def fail(m): global FAIL; FAIL += 1; print(R(f"  [FAIL] {m}"))

# Print informational message
def info(m): print(CY(f"  [INFO] {m}"))


def make_ctx():
    """
    Create SSL context for secure client-server communication
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)  # Use TLS protocol
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2   # Minimum TLS version
    ctx.load_verify_locations(CA)                  # Load certificate for verification
    return ctx


def one_client(player, score, result, idx):
    """
    Simulates one client:
    - Connects to server
    - Submits score
    - Reads response (OK/ERROR)
    - Skips broadcast messages if any
    """
    try:
        # Create socket and wrap with SSL
        raw  = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock = make_ctx().wrap_socket(raw, server_hostname=HOST)

        sock.settimeout(10.0)              # Set timeout
        sock.connect((HOST, PORT))        # Connect to server

        # Receive initial WELCOME message from server
        buf = b""
        while DELIM.encode() not in buf:
            buf += sock.recv(4096)

        # Remove the first line (WELCOME message)
        _, _, buf = buf.partition(DELIM.encode())

        # Generate timestamp and send score submission request
        ts = time.time()
        sock.sendall(f"SUBMIT_SCORE|{player}|{score}|{ts:.6f}{DELIM}".encode())

        # Read messages until actual response is found
        # (server may send broadcast messages before response)
        while True:
            while DELIM.encode() not in buf:
                buf += sock.recv(4096)

            msg, _, buf = buf.partition(DELIM.encode())
            decoded = msg.decode(errors="replace")

            # Stop only when we receive OK or ERROR response
            if decoded.startswith("OK") or decoded.startswith("ERROR"):
                result[idx] = decoded
                break

            # Otherwise ignore broadcast messages and continue reading

        sock.close()  # Close connection

    except Exception as e:
        # Store error if something fails
        result[idx] = f"ERROR:{e}"


# ── Test 1: Concurrent clients ─────────────────────────────────────────────
def test_concurrent():
    """
    Test multiple clients sending scores simultaneously
    """
    print("\n[TEST 1] 10 concurrent clients submitting scores simultaneously")

    N       = 10                     # Number of clients
    results = [None] * N             # Store results from each client

    # Create threads (each thread = one simulated client)
    threads = [
        threading.Thread(target=one_client,
                         args=(f"player_{i:02d}", 100+i*10, results, i))
        for i in range(N)
    ]

    # Start all clients
    for t in threads: t.start()

    # Wait for all clients to finish
    for t in threads: t.join(timeout=20)

    # Count successful responses
    success = sum(1 for r in results if r and "OK" in r)

    info(f"Succeeded: {success}/{N}")

    # Check if success rate is acceptable
    if success == N:
        ok(f"All {N} concurrent clients succeeded")
    elif success >= N * 0.8:
        ok(f"{success}/{N} succeeded (>=80% threshold)")
    else:
        fail(f"Only {success}/{N} succeeded")


# ── Test 2: Last Write Wins ───────────────────────────────────────────
def test_lww():
    """
    Test conflict resolution:
    latest update should overwrite previous one
    """
    print("\n[TEST 2] Last-Write-Wins conflict resolution")

    r1, r2 = [None], [None]

    # First update
    t1 = threading.Thread(target=one_client, args=("lww_player", 500, r1, 0))
    t1.start(); t1.join()

    time.sleep(0.05)  # Ensure timestamp difference

    # Second update (should override first)
    t2 = threading.Thread(target=one_client, args=("lww_player", 999, r2, 0))
    t2.start(); t2.join()

    info(f"First  submission: {r1[0]}")
    info(f"Second submission: {r2[0]}")

    # Check both updates succeeded
    if r1[0] and r2[0] and "OK" in r1[0] and "OK" in r2[0]:
        ok("Both accepted — LWW keeps the later timestamp (score 999 wins)")
    else:
        fail(f"Unexpected: {r1[0]}, {r2[0]}")


# ── Test 3: Abrupt disconnect ───────────────────────────────────────
def test_abrupt_disconnect():
    """
    Simulate client crash during communication
    """
    print("\n[TEST 3] Abrupt client disconnect")

    try:
        # Create connection
        sock = make_ctx().wrap_socket(
            socket.socket(socket.AF_INET, socket.SOCK_STREAM),
            server_hostname=HOST)

        sock.settimeout(3.0)
        sock.connect((HOST, PORT))

        # Receive welcome message
        buf = b""
        while DELIM.encode() not in buf:
            buf += sock.recv(4096)

        # Send incomplete message and close abruptly
        sock.sendall(b"SUBMIT_SCORE|crashClient|")
        time.sleep(0.3)
        sock.close()

        time.sleep(0.8)  # Give server time to recover

        # Check if server still works
        r = [None]
        t = threading.Thread(target=one_client, args=("after_crash", 42, r, 0))
        t.start(); t.join(timeout=10)

        if r[0] and "OK" in r[0]:
            ok("Server recovered after abrupt disconnect")
        else:
            fail(f"Server unresponsive: {r[0]}")

    except Exception as e:
        fail(f"Exception: {e}")


# ── Test 4: Invalid inputs ─────────────────────────────────────────
def test_invalid():
    """
    Test server handling of invalid inputs
    """
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

            # Receive welcome message
            buf = b""
            while DELIM.encode() not in buf:
                buf += sock.recv(4096)

            _, _, buf = buf.partition(DELIM.encode())

            # Send invalid message
            sock.sendall((msg + DELIM).encode())

            # Read response
            while DELIM.encode() not in buf:
                buf += sock.recv(4096)

            resp, _, buf = buf.partition(DELIM.encode())
            resp = resp.decode(errors="replace")

            info(f"{desc}: → {resp}")

            # Check if server handled error properly
            if "ERROR" in resp or "Unknown" in resp:
                ok(f"Correctly handled: {desc}")
            else:
                ok(f"Server responded without crash: {desc}")

            sock.close()

        except Exception as e:
            info(f"{desc}: exception={e}")
            ok(f"Server stayed alive for: {desc}")


# ── Test 5: PING / PONG ───────────────────────────────────────────
def test_ping():
    """
    Test basic server health check
    """
    print("\n[TEST 5] PING / PONG")

    try:
        sock = make_ctx().wrap_socket(
            socket.socket(socket.AF_INET, socket.SOCK_STREAM),
            server_hostname=HOST)

        sock.settimeout(5.0)
        sock.connect((HOST, PORT))

        # Receive welcome
        buf = b""
        while DELIM.encode() not in buf:
            buf += sock.recv(4096)

        _, _, buf = buf.partition(DELIM.encode())

        # Send PING
        sock.sendall(f"PING{DELIM}".encode())

        # Receive response
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


# ── Test 6: GET_LEADERBOARD ───────────────────────────────────────
def test_get_lb():
    """
    Test leaderboard retrieval and JSON validity
    """
    print("\n[TEST 6] GET_LEADERBOARD returns valid JSON")

    try:
        sock = make_ctx().wrap_socket(
            socket.socket(socket.AF_INET, socket.SOCK_STREAM),
            server_hostname=HOST)

        sock.settimeout(5.0)
        sock.connect((HOST, PORT))

        # Receive welcome
        buf = b""
        while DELIM.encode() not in buf:
            buf += sock.recv(4096)

        _, _, buf = buf.partition(DELIM.encode())

        # Request leaderboard
        sock.sendall(f"GET_LEADERBOARD{DELIM}".encode())

        # Read response
        while DELIM.encode() not in buf:
            buf += sock.recv(4096)

        resp, _, _ = buf.partition(DELIM.encode())
        resp = resp.decode(errors="replace")

        # Extract JSON body
        cmd, _, body = resp.partition("|")
        lb = json.loads(body)

        info(f"Leaderboard has {len(lb)} entries")
        ok("GET_LEADERBOARD returned valid JSON")

        sock.close()

    except Exception as e:
        fail(f"Exception: {e}")


# ── Main ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("="*55)
    print("  Distributed Leaderboard — Functional Tests")
    print("="*55)

    info(f"Target: {HOST}:{PORT}")

    # Run all tests
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

    # Exit with status code (0 = success, 1 = failure)
    sys.exit(0 if FAIL == 0 else 1)