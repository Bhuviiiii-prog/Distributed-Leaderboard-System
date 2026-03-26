#!/usr/bin/env python3
"""
Distributed Leaderboard Server
===============================
Phase 2 — Core Socket Implementation (8 marks)
Explicit: socket() → bind() → listen() → accept() → ssl.wrap → thread

Protocol (custom TCP, newline-delimited):
  SUBMIT_SCORE|<player>|<score>|<timestamp>
  GET_LEADERBOARD
  PING
"""

import socket
import ssl
import threading
import json
import time
import logging
import signal
import sys
from datetime import datetime

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("server.log"),
    ],
)
log = logging.getLogger(__name__)

# ── Global leaderboard (thread-safe via RW lock) ──────────────────────────────
leaderboard_lock  = threading.Lock()
leaderboard: dict = {}          # { player: {score, timestamp, submissions, last_updated} }

clients_lock      = threading.Lock()
connected_clients = []          # List of active ssl.SSLSocket connections

# ── Protocol ──────────────────────────────────────────────────────────────────
BUFFER_SIZE = 4096
DELIM       = "\n"
CMD_SUBMIT  = "SUBMIT_SCORE"
CMD_GET     = "GET_LEADERBOARD"
CMD_PING    = "PING"


# ── Leaderboard: Last-Write-Wins conflict resolution ─────────────────────────
def update_leaderboard(player: str, score: int, timestamp: float) -> bool:
    """
    Accepts write only if incoming timestamp > stored timestamp (LWW).
    Returns True if accepted, False if rejected (stale).
    """
    with leaderboard_lock:
        existing = leaderboard.get(player)
        if existing is None or timestamp > existing["timestamp"]:
            leaderboard[player] = {
                "score":        score,
                "timestamp":    timestamp,
                "submissions":  (existing["submissions"] + 1) if existing else 1,
                "last_updated": datetime.fromtimestamp(timestamp).isoformat(),
            }
            log.info(f"[LB UPDATE] {player}: {score} (ts={timestamp:.4f})")
            return True
        log.warning(f"[LB REJECT] Stale write for {player} (incoming ts={timestamp:.4f})")
        return False


def get_snapshot() -> list:
    """Thread-safe sorted leaderboard snapshot."""
    with leaderboard_lock:
        ranked = sorted(
            [{"rank": 0, "player": p, **v} for p, v in leaderboard.items()],
            key=lambda x: x["score"], reverse=True,
        )
        for i, e in enumerate(ranked, 1):
            e["rank"] = i
        return ranked


# ── Broadcast to all connected clients ───────────────────────────────────────
def broadcast():
    """Push leaderboard update to every live client; remove dead ones."""
    payload = f"LEADERBOARD_UPDATE|{json.dumps(get_snapshot())}{DELIM}".encode()
    dead = []
    with clients_lock:
        for sock in connected_clients:
            try:
                sock.sendall(payload)
            except Exception:
                dead.append(sock)
    if dead:
        with clients_lock:
            for s in dead:
                if s in connected_clients:
                    connected_clients.remove(s)
        log.info(f"[CLEANUP] Removed {len(dead)} dead client(s). Active: {len(connected_clients)}")


# ── Partial-read safe receive (handles TCP stream fragmentation) ──────────────
def recv_msg(sock: ssl.SSLSocket) -> str | None:
    """
    TCP does NOT guarantee full messages in one recv().
    Buffer until we find the newline delimiter.
    """
    buf = b""
    while True:
        try:
            chunk = sock.recv(BUFFER_SIZE)
            if not chunk:
                return None          # Clean disconnect
            buf += chunk
            if DELIM.encode() in buf:
                msg, _, _ = buf.partition(DELIM.encode())
                return msg.decode(errors="replace")
        except ssl.SSLWantReadError:
            time.sleep(0.001)
        except (ConnectionResetError, OSError):
            return None


# ── Per-client thread ─────────────────────────────────────────────────────────
def handle_client(conn: ssl.SSLSocket, addr: tuple):
    log.info(f"[CONNECT] {addr}  cipher={conn.cipher()[0]}")

    with clients_lock:
        connected_clients.append(conn)
    log.info(f"[ACTIVE] {len(connected_clients)} connection(s)")

    # Send current leaderboard as welcome
    try:
        conn.sendall(f"WELCOME|{json.dumps(get_snapshot())}{DELIM}".encode())
    except Exception as e:
        log.error(f"[WELCOME FAIL] {addr}: {e}")

    try:
        while True:
            raw = recv_msg(conn)
            if raw is None:
                break

            parts = raw.strip().split("|")
            cmd   = parts[0].upper()
            args  = parts[1:]

            # PING ─────────────────────────────────────────────────────────────
            if cmd == CMD_PING:
                conn.sendall(f"PONG{DELIM}".encode())

            # SUBMIT_SCORE ─────────────────────────────────────────────────────
            elif cmd == CMD_SUBMIT:
                if len(args) < 3:
                    conn.sendall(
                        f"ERROR|SUBMIT_SCORE requires player, score, timestamp{DELIM}".encode()
                    )
                    continue
                try:
                    player    = args[0].strip()
                    score     = int(args[1].strip())
                    timestamp = float(args[2].strip())
                    if not player:
                        raise ValueError("Player name cannot be empty")
                    if score < 0:
                        raise ValueError("Score must be non-negative")
                    if update_leaderboard(player, score, timestamp):
                        conn.sendall(f"OK|Score accepted{DELIM}".encode())
                        broadcast()
                    else:
                        conn.sendall(f"OK|Score rejected (stale, LWW){DELIM}".encode())
                except ValueError as e:
                    conn.sendall(f"ERROR|Invalid input: {e}{DELIM}".encode())

            # GET_LEADERBOARD ──────────────────────────────────────────────────
            elif cmd == CMD_GET:
                conn.sendall(
                    f"LEADERBOARD|{json.dumps(get_snapshot())}{DELIM}".encode()
                )

            # Unknown ──────────────────────────────────────────────────────────
            else:
                conn.sendall(f"ERROR|Unknown command '{cmd}'{DELIM}".encode())

    except ssl.SSLError as e:
        log.error(f"[SSL ERROR] {addr}: {e}")
    except Exception as e:
        log.error(f"[CLIENT ERROR] {addr}: {e}", exc_info=True)
    finally:
        with clients_lock:
            if conn in connected_clients:
                connected_clients.remove(conn)
        try:
            conn.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        conn.close()
        log.info(f"[CLOSED] {addr}  Active: {len(connected_clients)}")


# ── SSL Context ───────────────────────────────────────────────────────────────
def make_ssl_ctx(cert: str, key: str) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_cert_chain(certfile=cert, keyfile=key)
    log.info(f"[SSL] cert={cert}  key={key}")
    return ctx


# ── Main server loop ──────────────────────────────────────────────────────────
def run_server(host="0.0.0.0", port=9999,
               cert="certs/server.crt", key="certs/server.key"):

    ssl_ctx = make_ssl_ctx(cert, key)

    # 1. socket()
    raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    raw.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # 2. bind()
    raw.bind((host, port))
    log.info(f"[BIND] {host}:{port}")

    # 3. listen()  — backlog=200 to handle burst of concurrent clients
    raw.listen(200)
    log.info(f"[LISTEN] {host}:{port}  backlog=200")

    # Wrap the listening socket with SSL
    server_sock = ssl_ctx.wrap_socket(raw, server_side=True)
    log.info(f"[SERVER] Ready on port {port}. Press Ctrl+C to stop.\n")

    def shutdown(sig, frame):
        log.info("\n[SHUTDOWN] Stopping server...")
        try:
            server_sock.close()
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # 4. accept() loop
    while True:
        try:
            conn, addr = server_sock.accept()
        except ssl.SSLError as e:
            log.warning(f"[SSL HANDSHAKE FAIL] {e}")
            continue
        except OSError:
            break

        t = threading.Thread(
            target=handle_client,
            args=(conn, addr),
            name=f"C-{addr[1]}",
            daemon=True,
        )
        t.start()


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", default=9999, type=int)
    p.add_argument("--cert", default="certs/server.crt")
    p.add_argument("--key",  default="certs/server.key")
    a = p.parse_args()
    run_server(a.host, a.port, a.cert, a.key)
