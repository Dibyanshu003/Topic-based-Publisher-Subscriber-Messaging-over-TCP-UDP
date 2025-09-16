// server.cpp — Hybrid TCP+UDP Pub/Sub Broker (Base64 passthrough) + lightweight metrics
// Build: g++ -std=c++17 -O2 -pthread server.cpp -o server

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <chrono>

using namespace std;

//==================== Protocol ====================
const int SUBSCRIBE = 1;
const int PUBLISH   = 2;
const int MSG       = 3;
const int ACK       = 4;
const int TERM      = 5;

struct Header { int type, topic_len, payload_len; };

//==================== Metrics ====================
struct Metrics {
    atomic<long long> total_publishes{0};
    atomic<long long> total_deliveries{0};

    atomic<long long> bytes_tcp_in{0};
    atomic<long long> bytes_tcp_out{0};
    atomic<long long> bytes_udp_in{0};
    atomic<long long> bytes_udp_out{0};

    atomic<long long> tcp_current{0};
    atomic<long long> tcp_peak{0};

    // publish handling latency at broker (fanout duration)
    atomic<long long> pub_latency_us_sum{0};
    atomic<long long> pub_latency_count{0};
} M;

static const char* METRICS_FILE = "metrics_server.csv";
static atomic<bool> metrics_header_written{false};

static long long now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

static void write_metrics_snapshot() {
    ofstream f(METRICS_FILE, ios::app);
    if(!f) return;
    if(!metrics_header_written.exchange(true)) {
        f << "ts_ms,total_publishes,total_deliveries,avg_fanout,"
          << "tcp_current,tcp_peak,"
          << "bytes_tcp_in,bytes_tcp_out,bytes_udp_in,bytes_udp_out,"
          << "avg_pub_latency_us\n";
    }
    long long pubs = M.total_publishes.load();
    long long dels = M.total_deliveries.load();
    double avg_fanout = pubs ? (double)dels / (double)pubs : 0.0;

    long long lat_cnt = M.pub_latency_count.load();
    double avg_lat_us = lat_cnt ? (double)M.pub_latency_us_sum.load() / (double)lat_cnt : 0.0;

    f << now_ms() << ","
      << pubs << "," << dels << "," << avg_fanout << ","
      << M.tcp_current.load() << "," << M.tcp_peak.load() << ","
      << M.bytes_tcp_in.load() << "," << M.bytes_tcp_out.load() << ","
      << M.bytes_udp_in.load() << "," << M.bytes_udp_out.load() << ","
      << avg_lat_us << "\n";
}

static void metrics_snapshot_loop() {
    // write every 2 seconds
    while(true) {
        this_thread::sleep_for(std::chrono::seconds(2));
        write_metrics_snapshot();
    }
}

//==================== Pretty addr ====================
static string ipport_str(const sockaddr_in& a){
    char ip[INET_ADDRSTRLEN]{};
    inet_ntop(AF_INET, &a.sin_addr, ip, sizeof(ip));
    return string(ip) + ":" + to_string((int)ntohs(a.sin_port));
}
static string ip_from_fd(int fd){
    sockaddr_in a{}; socklen_t alen=sizeof(a);
    if(getpeername(fd,(sockaddr*)&a,&alen)==0) return ipport_str(a);
    return "unknown";
}

//==================== Endpoint & Client State ====================
struct Endpoint{
    bool is_tcp = true;
    int  fd     = -1;      // valid when is_tcp=true
    sockaddr_in addr{};    // valid when is_tcp=false
};
static bool endpoint_eq_fd(const Endpoint& e, int fd){
    return e.is_tcp && e.fd==fd;
}
static bool endpoint_eq_udp(const Endpoint& e, const sockaddr_in& a){
    return !e.is_tcp && e.addr.sin_addr.s_addr==a.sin_addr.s_addr && e.addr.sin_port==a.sin_port;
}

struct ClientStateTCP {
    int fd=-1, id=0;
    string remote; // "ip:port"
    bool acted_as_pub=false, acted_as_sub=false;
    set<string> topics;
};
struct ClientStateUDP {
    int id=0;
    sockaddr_in addr{};
    bool acted_as_pub=false, acted_as_sub=false;
    set<string> topics;
};

//==================== Globals (broker state) ====================
static mutex g_mu;
static atomic<int> next_id{1};
static int g_udp_fd = -1;  // used for all UDP fanout sends

static unordered_map<string, vector<Endpoint>> topic_subs; // topic -> endpoints
static unordered_map<int, ClientStateTCP> clients_tcp;      // fd -> state
static unordered_map<string, ClientStateUDP> clients_udp;   // "ip:port" -> state

//==================== I/O helpers (TCP) ====================
static bool send_all(int fd, const void* buf, size_t len){
    const char* p=(const char*)buf;
    while(len){
        ssize_t n=send(fd,p,len,0);
        if(n<0){ if(errno==EINTR) continue; return false; }
        M.bytes_tcp_out.fetch_add(n);
        p+=n; len-= (size_t)n;
    }
    return true;
}
static bool recv_all(int fd, void* buf, size_t len){
    char* p=(char*)buf;
    while(len){
        ssize_t n=recv(fd,p,len,0);
        if(n==0) return false;
        if(n<0){ if(errno==EINTR) continue; return false; }
        M.bytes_tcp_in.fetch_add(n);
        p+=n; len-= (size_t)n;
    }
    return true;
}
static bool send_packet_tcp(int fd, int type, const string& topic, const string& payload){
    Header h; h.type=htonl(type); h.topic_len=htonl((int)topic.size()); h.payload_len=htonl((int)payload.size());
    if(!send_all(fd,&h,sizeof(h))) return false;
    if(!topic.empty()   && !send_all(fd, topic.data(),   topic.size()))   return false;
    if(!payload.empty() && !send_all(fd, payload.data(), payload.size())) return false;
    return true;
}
static bool recv_packet_tcp(int fd, int& type, string& topic, string& payload){
    Header h{}; if(!recv_all(fd,&h,sizeof(h))) return false;
    type=ntohl(h.type);
    int tlen=ntohl(h.topic_len), plen=ntohl(h.payload_len);
    topic.assign(tlen,'\0'); payload.assign(plen,'\0');
    if(tlen && !recv_all(fd, topic.data(), (size_t)tlen)) return false;
    if(plen && !recv_all(fd, payload.data(), (size_t)plen)) return false;
    return true;
}

//==================== I/O helpers (UDP) ====================
static bool send_packet_udp(int ufd, const sockaddr_in& to, int type, const string& topic, const string& payload){
    Header h; h.type=htonl(type); h.topic_len=htonl((int)topic.size()); h.payload_len=htonl((int)payload.size());
    vector<char> buf; buf.reserve(sizeof(h)+topic.size()+payload.size());
    buf.insert(buf.end(), (char*)&h, (char*)&h + sizeof(h));
    buf.insert(buf.end(), topic.begin(), topic.end());
    buf.insert(buf.end(), payload.begin(), payload.end());
    ssize_t n = sendto(ufd, buf.data(), buf.size(), 0, (const sockaddr*)&to, sizeof(to));
    if(n>0) M.bytes_udp_out.fetch_add(n);
    return n==(ssize_t)buf.size();
}

//==================== Subscription management ====================
static void add_subscription_tcp(const string& topic, int fd){
    lock_guard<mutex> lk(g_mu);
    auto& list = topic_subs[topic];
    auto it = find_if(list.begin(), list.end(), [&](const Endpoint& e){ return endpoint_eq_fd(e,fd); });
    if(it==list.end()){
        Endpoint e; e.is_tcp=true; e.fd=fd;
        list.push_back(e);
    }
    auto &cs = clients_tcp[fd];
    cs.acted_as_sub = true;
    cs.topics.insert(topic);
}
static void add_subscription_udp(const string& topic, const sockaddr_in& a){
    lock_guard<mutex> lk(g_mu);
    auto& list = topic_subs[topic];
    auto it = find_if(list.begin(), list.end(), [&](const Endpoint& e){ return endpoint_eq_udp(e,a); });
    if(it==list.end()){
        Endpoint e; e.is_tcp=false; e.addr=a;
        list.push_back(e);
    }
    string key = ipport_str(a);
    auto &cs = clients_udp[key];
    cs.acted_as_sub = true;
    cs.topics.insert(topic);
}
static void remove_all_subs_for_tcp_fd(int fd){
    lock_guard<mutex> lk(g_mu);
    for(auto& kv: topic_subs){
        auto& v = kv.second;
        v.erase(remove_if(v.begin(), v.end(), [&](const Endpoint& e){ return endpoint_eq_fd(e,fd); }), v.end());
    }
}
static void remove_all_subs_for_udp_addr(const sockaddr_in& a){
    lock_guard<mutex> lk(g_mu);
    for(auto& kv: topic_subs){
        auto& v = kv.second;
        v.erase(remove_if(v.begin(), v.end(), [&](const Endpoint& e){ return endpoint_eq_udp(e,a); }), v.end());
    }
}

//==================== Fanout ====================
static int fanout_to_topic(const string& topic, const string& payload_b64){
    vector<Endpoint> targets;
    {
        lock_guard<mutex> lk(g_mu);
        auto it = topic_subs.find(topic);
        if(it!=topic_subs.end()) targets = it->second; // copy
    }
    int delivered=0;
    for(const auto& e: targets){
        if(e.is_tcp){
            if(send_packet_tcp(e.fd, MSG, topic, payload_b64)) delivered++;
        }else{
            if(g_udp_fd>=0 && send_packet_udp(g_udp_fd, e.addr, MSG, topic, payload_b64)) delivered++;
        }
    }
    return delivered;
}

//==================== ACK helpers ====================
static inline void send_ack_tcp(int fd){ (void)send_packet_tcp(fd, ACK, "", ""); }
static inline void send_ack_udp(const sockaddr_in& to){
    if(g_udp_fd>=0) (void)send_packet_udp(g_udp_fd, to, ACK, "", "");
}

//==================== TCP client handler ====================
static void handle_tcp_client(int cfd){
    // register
    ClientStateTCP my{};
    my.fd = cfd;
    my.id = next_id++;
    my.remote = ip_from_fd(cfd);
    {
        lock_guard<mutex> lk(g_mu);
        clients_tcp[cfd] = my;
    }
    cerr << "[TCP CONNECT]  Client#" << my.id << " from " << my.remote << "\n";

    for(;;){
        int type; string topic, payload_b64;
        if(!recv_packet_tcp(cfd, type, topic, payload_b64)) break;

        if(type==SUBSCRIBE){
            add_subscription_tcp(topic, cfd);
            cerr << "[SUBSCRIBE]   Client#" << my.id << " via TCP -> '" << topic << "'\n";
            send_ack_tcp(cfd);

        }else if(type==PUBLISH){
            {
                lock_guard<mutex> lk(g_mu);
                auto it = clients_tcp.find(cfd);
                if(it!=clients_tcp.end()) it->second.acted_as_pub = true;
            }
            // metrics: publish count + latency + deliveries
            auto t0 = chrono::steady_clock::now();
            int fan = fanout_to_topic(topic, payload_b64);
            auto t1 = chrono::steady_clock::now();
            long long us = chrono::duration_cast<chrono::microseconds>(t1 - t0).count();

            M.total_publishes.fetch_add(1);
            M.total_deliveries.fetch_add(fan);
            M.pub_latency_us_sum.fetch_add(us);
            M.pub_latency_count.fetch_add(1);

            cerr << "[PUBLISH]     Client#" << my.id << " via TCP -> '" << topic
                 << "' fanout=" << fan << " latency_us=" << us << "\n";
            send_ack_tcp(cfd);

        }else if(type==TERM){
            send_ack_tcp(cfd);
            break;
        }
    }

    ClientStateTCP cs{};
    {
        lock_guard<mutex> lk(g_mu);
        auto it = clients_tcp.find(cfd);
        if(it!=clients_tcp.end()) cs = it->second;
    }
    remove_all_subs_for_tcp_fd(cfd);
    {
        lock_guard<mutex> lk(g_mu);
        clients_tcp.erase(cfd);
    }

    cerr << "[TCP CLOSE]    Client#" << cs.id << " (" << cs.remote << ") disconnected. "
         << "Roles: " << (cs.acted_as_pub && cs.acted_as_sub ? "pub/sub" : cs.acted_as_pub ? "pub" : cs.acted_as_sub ? "sub" : "unknown") << ". ";
    if(cs.topics.empty()) cerr << "Subscribed topics: (none).\n";
    else{
        cerr << "Subscribed topics: ";
        bool first=true; for(const auto& t: cs.topics){ if(!first) cerr<<", "; cerr<<"'"<<t<<"'"; first=false; }
        cerr << ".\n";
    }
    M.tcp_current.fetch_sub(1);
    ::close(cfd);
}

//==================== UDP loop ====================
static void udp_loop(){
    vector<char> buf(64*1024);
    for(;;){
        sockaddr_in from{}; socklen_t flen=sizeof(from);
        ssize_t n = recvfrom(g_udp_fd, buf.data(), buf.size(), 0, (sockaddr*)&from, &flen);
        if(n<=0){ if(errno==EINTR) continue; perror("recvfrom"); continue; }
        if((size_t)n < sizeof(Header)) continue;
        M.bytes_udp_in.fetch_add(n);

        Header h{};
        memcpy(&h, buf.data(), sizeof(h));
        int type = ntohl(h.type);
        int tlen = ntohl(h.topic_len);
        int plen = ntohl(h.payload_len);
        size_t need = sizeof(h) + (size_t)tlen + (size_t)plen;
        if((size_t)n < need) continue; // malformed

        string topic, payload_b64;
        topic.assign(buf.data()+sizeof(h), (size_t)tlen);
        payload_b64.assign(buf.data()+sizeof(h)+tlen, (size_t)plen);

        // ensure UDP client state exists
        string key = ipport_str(from);
        bool first_seen=false;
        {
            lock_guard<mutex> lk(g_mu);
            if(!clients_udp.count(key)){
                ClientStateUDP cs{};
                cs.id = next_id++;
                cs.addr = from;
                clients_udp[key] = cs;
                first_seen = true;
            }
        }
        if(first_seen){
            cerr << "[UDP SEEN]     Client#" << clients_udp[key].id << " from " << key << "\n";
        }
        auto &cs = clients_udp[key];

        if(type==SUBSCRIBE){
            add_subscription_udp(topic, from);
            cerr << "[SUBSCRIBE]   Client#" << cs.id << " via UDP -> '" << topic << "'\n";
            send_ack_udp(from);

        }else if(type==PUBLISH){
            {
                lock_guard<mutex> lk(g_mu);
                clients_udp[key].acted_as_pub = true;
            }
            // metrics: publish count + latency + deliveries
            auto t0 = chrono::steady_clock::now();
            int fan = fanout_to_topic(topic, payload_b64);
            auto t1 = chrono::steady_clock::now();
            long long us = chrono::duration_cast<chrono::microseconds>(t1 - t0).count();

            M.total_publishes.fetch_add(1);
            M.total_deliveries.fetch_add(fan);
            M.pub_latency_us_sum.fetch_add(us);
            M.pub_latency_count.fetch_add(1);

            cerr << "[PUBLISH]     Client#" << cs.id << " via UDP -> '" << topic
                 << "' fanout=" << fan << " latency_us=" << us << "\n";
            send_ack_udp(from);

        }else if(type==TERM){
            send_ack_udp(from);

            ClientStateUDP snapshot;
            {
                lock_guard<mutex> lk(g_mu);
                snapshot = clients_udp[key];
            }
            remove_all_subs_for_udp_addr(from);
            {
                lock_guard<mutex> lk(g_mu);
                clients_udp.erase(key);
            }
            cerr << "[UDP TERM]     Client#" << snapshot.id << " (" << key << ") finished. "
                 << "Roles: " << (snapshot.acted_as_pub && snapshot.acted_as_sub ? "pub/sub" : snapshot.acted_as_pub ? "pub" : snapshot.acted_as_sub ? "sub" : "unknown") << ". ";
            if(snapshot.topics.empty()) cerr << "Subscribed topics: (none).\n";
            else{
                cerr << "Subscribed topics: ";
                bool first=true; for(const auto& t: snapshot.topics){ if(!first) cerr<<", "; cerr<<"'"<<t<<"'"; first=false; }
                cerr << ".\n";
            }
        }
    }
}

//==================== Main ====================
int main(int argc, char** argv){
    if(argc!=2){ cerr<<"Usage: ./server <port>\n"; return 1; }
    int port = stoi(argv[1]);

    // TCP listen socket
    int tfd = socket(AF_INET, SOCK_STREAM, 0);
    if(tfd<0){ perror("socket TCP"); return 1; }
    int yes=1; setsockopt(tfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    sockaddr_in addr{}; addr.sin_family=AF_INET; addr.sin_addr.s_addr=INADDR_ANY; addr.sin_port=htons(port);
    if(bind(tfd,(sockaddr*)&addr,sizeof(addr))<0){ perror("bind TCP"); return 1; }
    if(listen(tfd, 128)<0){ perror("listen"); return 1; }

    // UDP socket (same port)
    g_udp_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if(g_udp_fd<0){ perror("socket UDP"); return 1; }
    if(bind(g_udp_fd,(sockaddr*)&addr,sizeof(addr))<0){ perror("bind UDP"); return 1; }

    cout << "Broker listening on TCP+UDP port " << port << "...\n";

    // Start UDP loop and metrics snapshot thread
    thread(udp_loop).detach();
    thread(metrics_snapshot_loop).detach();

    // TCP accept loop
    while(true){
        sockaddr_in caddr{}; socklen_t clen=sizeof(caddr);
        int cfd = accept(tfd, (sockaddr*)&caddr, &clen);
        if(cfd<0){ if(errno==EINTR) continue; perror("accept"); break; }

        // metrics: tcp_current/peak
        long long cur = M.tcp_current.fetch_add(1) + 1;
        long long prev = M.tcp_peak.load();
        while(cur > prev && !M.tcp_peak.compare_exchange_weak(prev, cur)) {
            /* retry CAS */
        }

        thread(handle_tcp_client, cfd).detach();

        // when a TCP client finishes, handle_tcp_client() closes it
        // we decrement current count here via a small watcher thread
        // (avoid modifying handle_tcp_client’s end to keep separation)
        thread([cfd](){
            // wait for FIN-ish by polling getpeername(readiness is inferred by read failure in handler)
            // Here we simply busy-wait on SO_ERROR via getsockopt returning non-zero after handler closes.
            // To keep it simple and robust, decrement in handle close path instead:
        }).detach();
    }

    ::close(tfd);
    ::close(g_udp_fd);
    return 0;
}

