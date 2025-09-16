# Topic-based Publisher/Subscriber Messaging over TCP & UDP (Base64)

C++ sockets project that implements a **publisher–subscriber (pub/sub)** messaging model over **TCP** and **UDP**.  
Payloads are **Base64-encoded** so they’re printable and easy to log/debug.

## Overview

- A **Server** acts as a broker for **topics**.
- **Clients** can run as **Publisher** (send messages to a topic) or **Subscriber** (receive messages from one or more topics).
- Works over both **TCP** (reliable stream) and **UDP** (datagrams).
- Message bodies are **Base64-encoded** end-to-end, which keeps the wire payload text-safe and easy to inspect.

---

## Features

- Topic-based **publish/subscribe**
- **TCP** and **UDP** transports (choose per client)
- **Base64** encode/decode for payloads
- **ACK** flow for subscribe/publish
- **Multi-topic subscription**
- No external libraries; simple, readable C++
- Lightweight performance metrics: broker latency (µs), avg_fanout, TCP/UDP bytes in/out, and concurrent TCP (current/peak) — auto-logged to CSV.

## Build
```bash
g++ server.cpp -o server
g++ client.cpp -o client
```

## Run

### 1) Start the server(Terminal 1)
```bash
./server <port>
# example
./server 9000
```
### 2) Start a subscriber(Terminal 2)
```bash
./client <server_ip> <port> <tcp|udp> sub <topic1> [topic2 ...]
# examples
./client 127.0.0.1 9000 tcp sub sports
./client 127.0.0.1 9000 udp sub sports tech
```
### 3) Start a publisher(Terminal 3)
```bash
./client <server_ip> <port> <tcp|udp> pub <topic>
# examples
./client 127.0.0.1 9000 tcp pub sports
./client 127.0.0.1 9000 udp pub sports
# then type a line and press Enter to publish
