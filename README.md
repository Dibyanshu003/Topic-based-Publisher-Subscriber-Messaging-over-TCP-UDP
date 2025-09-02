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


## Run

### 1) Start the server
```bash
./server <port>
# example
./server 9000
