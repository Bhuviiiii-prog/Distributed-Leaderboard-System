# Distributed-Leaderboard-System
Maintain real-time rankings updated by multiple clients.

## Objectives

This project is designed to satisfy the following conditions:

1. Concurrent score updates
2. Consistency guarantees
3. Conflict resolution (Last-Write-Wins)
4. Performance under high update rate

## Project Structure 
leaderboard/  
├── server/  
│   └── server.py  
├── client_python/  
│   └── client.py  
├── tests/  
│   ├── test_concurrent.py  
│   └── load_test.py  
├── certs/  
│   ├── gen_certs.sh  
│   ├── san.cnf  
│   ├── server.crt          # generated locally  
│   └── server.key          # generated locally  
├── docs/  
│   └── protocol.md  
└── README.md  

## Setup

### Step 1-Clone Repository

```bash
git clone <your-repo-url>
cd Distributed-Leaderboard-System
```

### Step 2 -Generate TLS certificates

```bash
cd certs
bash gen_certs.sh
cd ..
```

### Step 3- Optional dependency (for performance plotting)

`matplotlib` is only needed for plotting load-test graphs.

```bash
pip3 install matplotlib --break-system-packages
```
### Step 4 — Start Server (Terminal 1)
```bash
python3 server/server.py
```

### Step 5 — Connect Client (Terminal 2)
```bash
python3 client_python/client.py
>> submit alice 850
>> submit bob 500
>> get
>> ping
>> quit
```

### Step 6 — Second Client to see broadcasts (Terminal 3)
```bash
python3 client_python/client.py
# When another client submits, you'll see LEADERBOARD_UPDATE here automatically
```

### Step 7 — Run Functional Tests and Load Test (Terminal 4)
```bash
python3 tests/test_concurrent.py
python3 tests/load_test.py --levels 1 5 10 20 50 --requests 20
```

## Development Roadmap

- [ ] Implement TLS server in server/server.py
- [ ] Implement interactive client in client_python/client.py
- [ ] Finalize wire protocol in docs/protocol.md
- [ ] Add concurrency tests in tests/test_concurrent.py
- [ ] Add load tests in tests/load_test.py

##Team Members
Bollavaram Santosh Kumar
Bheema Varshini
Bhuvigna Reddy A T

