# TODO: Add load test to measure throughput and latency under high update rates.

#!/usr/bin/env python3  # Run using Python 3

"""
load_test.py — Performance Evaluation
Measures throughput, avg latency, P99 latency across concurrency levels.
Generates matplotlib graphs saved to performance/performance_results.png
"""

# Import required modules for networking, security, timing, threading, stats, CLI args, etc.
import socket, ssl, time, threading, statistics, argparse, json, sys
from pathlib import Path

# Try importing matplotlib (used for generating graphs)
try:
    import matplotlib
    matplotlib.use("Agg")  # Use non-GUI backend (for saving images)
    import matplotlib.pyplot as plt
    HAS_PLT = True
except ImportError:
    HAS_PLT = False
    print("[WARN] matplotlib not found. Install it with:")
    print("       pip3 install matplotlib --break-system-packages\n")

# Message delimiter (end of each message)
DELIM  = "\n"

# Buffer size for receiving data
BUFLEN = 4096


def worker(host, port, cafile, player, n, latencies, errors, lock):
    """
    Simulates one client:
    - Connects to server
    - Sends 'n' score updates
    - Measures latency for each request
    """

    # Create SSL context for secure communication
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_verify_locations(cafile)

    try:
        # Create TCP socket and wrap it with SSL
        raw  = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock = ctx.wrap_socket(raw, server_hostname=host)

        # Set timeout and connect to server
        sock.settimeout(15.0)
        sock.connect((host, port))

        # Receive initial WELCOME message from server
        buf = b""
        while DELIM.encode() not in buf:
            buf += sock.recv(BUFLEN)

        # Send multiple requests
        for i in range(n):
            # Generate score (unique per player + iteration)
            score = abs(hash(player)) % 1000 + i

            # Current timestamp
            ts    = time.time()

            # Format request message (protocol format)
            msg   = f"SUBMIT_SCORE|{player}|{score}|{ts:.6f}{DELIM}".encode()

            # Start timer
            t0 = time.perf_counter()

            # Send request to server
            sock.sendall(msg)

            # Wait for response from server
            rb = b""
            while DELIM.encode() not in rb:
                rb += sock.recv(BUFLEN)

            # End timer
            t1 = time.perf_counter()

            # Store latency in milliseconds (thread-safe using lock)
            with lock:
                latencies.append((t1 - t0) * 1000)

        # Close connection
        sock.close()

    except Exception as e:
        # Store any errors encountered
        with lock:
            errors.append(str(e))


def run_level(host, port, cafile, n_clients, n_req):
    """
    Runs load test for a given number of concurrent clients
    """

    # Shared lists for storing results
    latencies, errors, lock = [], [], threading.Lock()
    threads = []

    # Create multiple client threads (each thread = one simulated client)
    for i in range(n_clients):
        t = threading.Thread(
            target=worker,
            args=(host, port, cafile, f"bot_{i:03d}", n_req,
                  latencies, errors, lock),
            daemon=True,  # Thread will exit when main program exits
        )
        threads.append(t)

    # Start timer for entire test
    t0 = time.perf_counter()

    # Start all client threads
    for t in threads: t.start()

    # Wait for all threads to complete
    for t in threads: t.join(timeout=120)

    # Total time taken
    elapsed = time.perf_counter() - t0

    # Calculate performance metrics
    ok  = len(latencies)  # successful requests
    thr = ok / elapsed if elapsed > 0 else 0  # throughput (req/sec)
    avg = statistics.mean(latencies) if latencies else 0  # average latency
    p50 = statistics.median(latencies) if latencies else 0  # median latency
    p99 = sorted(latencies)[int(len(latencies) * 0.99)] if latencies else 0  # 99th percentile
    err = (len(errors) / (n_clients * n_req)) * 100 if n_clients * n_req else 0  # error rate %

    # Print results for this level
    print(f"  Clients={n_clients:>4} | Throughput={thr:>9.1f} req/s | "
          f"Avg={avg:>7.2f}ms | P99={p99:>7.2f}ms | Errors={len(errors)}")

    # Return results as dictionary
    return dict(clients=n_clients, success=ok, errors=len(errors),
                error_rate=round(err,2), elapsed_s=round(elapsed,3),
                throughput=round(thr,2), avg_lat_ms=round(avg,3),
                p50_lat_ms=round(p50,3), p99_lat_ms=round(p99,3))


def plot(results, outdir="performance"):
    """
    Generates graphs:
    - Throughput vs Clients
    - Latency vs Clients
    - Error Rate vs Clients
    """

    # Skip if matplotlib not installed
    if not HAS_PLT:
        print("\n[GRAPH] Skipped — install matplotlib to generate graphs.")
        return

    # Create output directory
    Path(outdir).mkdir(exist_ok=True)

    # Extract data from results
    c  = [r["clients"]    for r in results]
    th = [r["throughput"] for r in results]
    av = [r["avg_lat_ms"] for r in results]
    p9 = [r["p99_lat_ms"] for r in results]
    er = [r["error_rate"] for r in results]

    # Create 3 graphs
    fig, ax = plt.subplots(1, 3, figsize=(15, 5))

    fig.suptitle("Distributed Leaderboard — Performance Evaluation",
                 fontsize=14, fontweight="bold")

    # Graph 1: Throughput
    ax[0].plot(c, th, "o-", color="steelblue", lw=2)
    ax[0].fill_between(c, th, alpha=0.15, color="steelblue")
    ax[0].set(title="Throughput vs Clients", xlabel="Concurrent Clients",
              ylabel="Requests / Second")
    ax[0].grid(alpha=0.4)

    # Graph 2: Latency
    ax[1].plot(c, av, "s-", label="Avg",  color="green",  lw=2)
    ax[1].plot(c, p9, "^--",label="P99",  color="orange", lw=2)
    ax[1].set(title="Latency vs Clients", xlabel="Concurrent Clients",
              ylabel="Latency (ms)")
    ax[1].legend()
    ax[1].grid(alpha=0.4)

    # Graph 3: Error Rate
    colors = ["red" if e > 0 else "green" for e in er]
    ax[2].bar(c, er, color=colors, alpha=0.7)
    ax[2].set(title="Error Rate vs Clients", xlabel="Concurrent Clients",
              ylabel="Error Rate (%)")
    ax[2].grid(alpha=0.4, axis="y")

    # Save graph image
    plt.tight_layout()
    out = f"{outdir}/performance_results.png"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    print(f"\n[GRAPH] Saved → {out}")
    plt.close()


if __name__ == "__main__":
    # Parse command-line arguments
    ap = argparse.ArgumentParser()
    ap.add_argument("--host",     default="127.0.0.1")
    ap.add_argument("--port",     default=9999, type=int)
    ap.add_argument("--cert",     default="certs/server.crt")
    ap.add_argument("--levels",   default=[1,5,10,20,50], nargs="+", type=int)
    ap.add_argument("--requests", default=20, type=int)
    a = ap.parse_args()

    # Print test configuration
    print("="*70)
    print("  Distributed Leaderboard — Load Test")
    print(f"  Target: {a.host}:{a.port}  |  Requests/client: {a.requests}")
    print("="*70)

    results = []

    # Run tests for different concurrency levels
    for n in a.levels:
        results.append(run_level(a.host, a.port, a.cert, n, a.requests))

    # Save results to JSON file
    Path("performance").mkdir(exist_ok=True)
    rpath = "performance/load_test_report.json"
    with open(rpath, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n[REPORT] Saved → {rpath}")

    # Print summary table
    print("\n" + "="*70)
    print("  SUMMARY")
    print("="*70)
    print(f"  {'Clients':>8} | {'Throughput (req/s)':>20} | {'Avg (ms)':>10} | {'P99 (ms)':>10}")
    print("  " + "-"*58)

    for r in results:
        print(f"  {r['clients']:>8} | {r['throughput']:>20.1f} | "
              f"{r['avg_lat_ms']:>10.2f} | {r['p99_lat_ms']:>10.2f}")

    # Generate graphs
    plot(results)