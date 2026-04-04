#!/usr/bin/env python3
"""
load_test.py — Performance Evaluation
Measures throughput, avg latency, P99 latency across concurrency levels.
Generates matplotlib graphs saved to performance/performance_results.png
"""

import socket, ssl, time, threading, statistics, argparse, json, sys
from pathlib import Path

# Try importing matplotlib
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_PLT = True
except ImportError:
    HAS_PLT = False
    print("[WARN] matplotlib not found. Install it with:")
    print("       pip3 install matplotlib --break-system-packages\n")

DELIM  = "\n"
BUFLEN = 4096


def worker(host, port, cafile, player, n, latencies, errors, lock):
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_verify_locations(cafile)
    try:
        raw  = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock = ctx.wrap_socket(raw, server_hostname=host)
        sock.settimeout(15.0)
        sock.connect((host, port))

        # drain WELCOME
        buf = b""
        while DELIM.encode() not in buf:
            buf += sock.recv(BUFLEN)

        for i in range(n):
            score = abs(hash(player)) % 1000 + i
            ts    = time.time()
            msg   = f"SUBMIT_SCORE|{player}|{score}|{ts:.6f}{DELIM}".encode()

            t0 = time.perf_counter()
            sock.sendall(msg)
            rb = b""
            while DELIM.encode() not in rb:
                rb += sock.recv(BUFLEN)
            t1 = time.perf_counter()

            with lock:
                latencies.append((t1 - t0) * 1000)

        sock.close()
    except Exception as e:
        with lock:
            errors.append(str(e))


def run_level(host, port, cafile, n_clients, n_req):
    latencies, errors, lock = [], [], threading.Lock()
    threads = []

    for i in range(n_clients):
        t = threading.Thread(
            target=worker,
            args=(host, port, cafile, f"bot_{i:03d}", n_req,
                  latencies, errors, lock),
            daemon=True,
        )
        threads.append(t)

    t0 = time.perf_counter()
    for t in threads: t.start()
    for t in threads: t.join(timeout=120)
    elapsed = time.perf_counter() - t0

    ok  = len(latencies)
    thr = ok / elapsed if elapsed > 0 else 0
    avg = statistics.mean(latencies) if latencies else 0
    p50 = statistics.median(latencies) if latencies else 0
    p99 = sorted(latencies)[int(len(latencies) * 0.99)] if latencies else 0
    err = (len(errors) / (n_clients * n_req)) * 100 if n_clients * n_req else 0

    print(f"  Clients={n_clients:>4} | Throughput={thr:>9.1f} req/s | "
          f"Avg={avg:>7.2f}ms | P99={p99:>7.2f}ms | Errors={len(errors)}")

    return dict(clients=n_clients, success=ok, errors=len(errors),
                error_rate=round(err,2), elapsed_s=round(elapsed,3),
                throughput=round(thr,2), avg_lat_ms=round(avg,3),
                p50_lat_ms=round(p50,3), p99_lat_ms=round(p99,3))


def plot(results, outdir="performance"):
    if not HAS_PLT:
        print("\n[GRAPH] Skipped — install matplotlib to generate graphs.")
        return
    Path(outdir).mkdir(exist_ok=True)
    c  = [r["clients"]    for r in results]
    th = [r["throughput"] for r in results]
    av = [r["avg_lat_ms"] for r in results]
    p9 = [r["p99_lat_ms"] for r in results]
    er = [r["error_rate"] for r in results]

    fig, ax = plt.subplots(1, 3, figsize=(15, 5))
    fig.suptitle("Distributed Leaderboard — Performance Evaluation",
                 fontsize=14, fontweight="bold")

    ax[0].plot(c, th, "o-", color="steelblue", lw=2)
    ax[0].fill_between(c, th, alpha=0.15, color="steelblue")
    ax[0].set(title="Throughput vs Clients", xlabel="Concurrent Clients",
              ylabel="Requests / Second"); ax[0].grid(alpha=0.4)

    ax[1].plot(c, av, "s-", label="Avg",  color="green",  lw=2)
    ax[1].plot(c, p9, "^--",label="P99",  color="orange", lw=2)
    ax[1].set(title="Latency vs Clients", xlabel="Concurrent Clients",
              ylabel="Latency (ms)"); ax[1].legend(); ax[1].grid(alpha=0.4)

    colors = ["red" if e > 0 else "green" for e in er]
    ax[2].bar(c, er, color=colors, alpha=0.7)
    ax[2].set(title="Error Rate vs Clients", xlabel="Concurrent Clients",
              ylabel="Error Rate (%)"); ax[2].grid(alpha=0.4, axis="y")

    plt.tight_layout()
    out = f"{outdir}/performance_results.png"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    print(f"\n[GRAPH] Saved → {out}")
    plt.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host",     default="127.0.0.1")
    ap.add_argument("--port",     default=9999, type=int)
    ap.add_argument("--cert",     default="certs/server.crt")
    ap.add_argument("--levels",   default=[1,5,10,20,50], nargs="+", type=int)
    ap.add_argument("--requests", default=20, type=int)
    a = ap.parse_args()

    print("="*70)
    print("  Distributed Leaderboard — Load Test")
    print(f"  Target: {a.host}:{a.port}  |  Requests/client: {a.requests}")
    print("="*70)

    results = []
    for n in a.levels:
        results.append(run_level(a.host, a.port, a.cert, n, a.requests))

    Path("performance").mkdir(exist_ok=True)
    rpath = "performance/load_test_report.json"
    with open(rpath, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n[REPORT] Saved → {rpath}")

    print("\n" + "="*70)
    print("  SUMMARY")
    print("="*70)
    print(f"  {'Clients':>8} | {'Throughput (req/s)':>20} | {'Avg (ms)':>10} | {'P99 (ms)':>10}")
    print("  " + "-"*58)
    for r in results:
        print(f"  {r['clients']:>8} | {r['throughput']:>20.1f} | "
              f"{r['avg_lat_ms']:>10.2f} | {r['p99_lat_ms']:>10.2f}")

    plot(results)
