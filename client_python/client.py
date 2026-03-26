#!/usr/bin/env python3
"""
Distributed Leaderboard - Python Client
Connects via TLS using ssl.SSLContext.
"""

import socket, ssl, time, json, threading, sys, logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [CLIENT] %(message)s")
log = logging.getLogger(__name__)

DELIM  = "\n"
BUFLEN = 4096


class LeaderboardClient:
    def __init__(self, host="127.0.0.1", port=9999, cafile="certs/server.crt"):
        self.host    = host
        self.port    = port
        self.cafile  = cafile
        self.sock    = None
        self._alive  = False

    def _ssl_ctx(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.load_verify_locations(self.cafile)
        return ctx

    def connect(self):
        try:
            raw       = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock = self._ssl_ctx().wrap_socket(raw, server_hostname=self.host)
            self.sock.connect((self.host, self.port))
            self._alive = True
            log.info(f"Connected  cipher={self.sock.cipher()[0]}")
            threading.Thread(target=self._recv_loop, daemon=True).start()
            return True
        except Exception as e:
            log.error(f"Connect failed: {e}")
            return False

    def _recv_loop(self):
        buf = b""
        while self._alive:
            try:
                chunk = self.sock.recv(BUFLEN)
                if not chunk:
                    self._alive = False
                    break
                buf += chunk
                while DELIM.encode() in buf:
                    msg, _, buf = buf.partition(DELIM.encode())
                    self._on_msg(msg.decode(errors="replace"))
            except ssl.SSLWantReadError:
                time.sleep(0.001)
            except Exception:
                self._alive = False
                break

    def _on_msg(self, raw):
        raw   = raw.strip()
        sep   = raw.find("|")
        cmd   = raw[:sep] if sep >= 0 else raw
        body  = raw[sep+1:] if sep >= 0 else ""
        if cmd == "WELCOME":
            print("\n[SERVER] Welcome! Current leaderboard:")
            self._show(json.loads(body))
        elif cmd == "LEADERBOARD_UPDATE":
            print("\n[BROADCAST] Leaderboard updated:")
            self._show(json.loads(body))
        elif cmd == "LEADERBOARD":
            print("\n[LEADERBOARD]")
            self._show(json.loads(body))
        elif cmd == "OK":
            print(f"[OK] {body}")
        elif cmd == "PONG":
            print("[PONG] Server alive!")
        elif cmd == "ERROR":
            print(f"[ERROR] {body}")

    @staticmethod
    def _show(lb):
        if not lb:
            print("  (empty)")
            return
        print(f"  {'Rank':<5} {'Player':<20} {'Score'}")
        print("  " + "-"*40)
        for e in lb:
            print(f"  {e['rank']:<5} {e['player']:<20} {e['score']}")

    def submit(self, player, score):
        self._send(f"SUBMIT_SCORE|{player}|{score}|{time.time():.6f}")

    def get(self):
        self._send("GET_LEADERBOARD")

    def ping(self):
        self._send("PING")

    def _send(self, msg):
        if self.sock:
            self.sock.sendall((msg + DELIM).encode())

    def disconnect(self):
        self._alive = False
        if self.sock:
            try: self.sock.shutdown(socket.SHUT_RDWR)
            except Exception: pass
            self.sock.close()
        print("Disconnected.")


def interactive(client):
    print("\nCommands:  submit <player> <score>  |  get  |  ping  |  quit\n")
    while client._alive:
        try:
            line = input(">> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not line:
            continue
        p = line.split()
        if p[0] == "submit" and len(p) == 3:
            try:
                client.submit(p[1], int(p[2]))
            except ValueError:
                print("Score must be a number.")
        elif p[0] == "get":
            client.get()
        elif p[0] == "ping":
            client.ping()
        elif p[0] in ("quit","exit","q"):
            break
        else:
            print("Unknown command.")
        time.sleep(0.25)
    client.disconnect()


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--host",   default="127.0.0.1")
    ap.add_argument("--port",   default=9999, type=int)
    ap.add_argument("--cert",   default="certs/server.crt")
    ap.add_argument("--player", default=None)
    ap.add_argument("--score",  default=None, type=int)
    a = ap.parse_args()

    c = LeaderboardClient(a.host, a.port, a.cert)
    if not c.connect():
        sys.exit(1)
    time.sleep(0.4)
    if a.player and a.score is not None:
        c.submit(a.player, a.score)
        time.sleep(0.5)
        c.disconnect()
    else:
        interactive(c)
