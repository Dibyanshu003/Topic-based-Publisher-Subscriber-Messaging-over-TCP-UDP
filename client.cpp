// client.cpp — Hybrid TCP/UDP Pub/Sub Client (Base64), multi-topic subscribe
// Build: g++ -std=c++17 -O2 -pthread client.cpp -o client
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>
#include <chrono>

using namespace std;

// Protocol 
const int SUBSCRIBE = 1;
const int PUBLISH   = 2;
const int MSG       = 3;
const int ACK       = 4;
const int TERM      = 5;

struct Header { int type, topic_len, payload_len; };

// I/O helpers (TCP) 
static bool send_all(int fd, const void* buf, size_t len){
    const char* p=(const char*)buf;
    while(len){
        ssize_t n=send(fd,p,len,0);
        if(n<0){ if(errno==EINTR) continue; return false; }
        p+=n; len-= (size_t)n;
    }
    return true;
}
static bool recv_all(int fd, void* buf, size_t len){
    char* p=(char*)buf;
    while(len){
        ssize_t n=recv(fd,p,len,0);
        if(n==0) return false; // closed
        if(n<0){ if(errno==EINTR) continue; return false; }
        p+=n; len-= (size_t)n;
    }
    return true;
}

// TCP packet send/recv
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

// I/O helpers (UDP)
// Build datagram = Header || topic || payload
static bool send_packet_udp(int ufd, const sockaddr_in& to, int type, const string& topic, const string& payload){
    Header h; h.type=htonl(type); h.topic_len=htonl((int)topic.size()); h.payload_len=htonl((int)payload.size());
    vector<char> buf; buf.reserve(sizeof(h)+topic.size()+payload.size());
    buf.insert(buf.end(), (char*)&h, (char*)&h + sizeof(h));
    buf.insert(buf.end(), topic.begin(), topic.end());
    buf.insert(buf.end(), payload.begin(), payload.end());
    ssize_t n = sendto(ufd, buf.data(), buf.size(), 0, (const sockaddr*)&to, sizeof(to));
    return n==(ssize_t)buf.size();
}

// Parse one datagram into (type, topic, payload); returns false if malformed
static bool parse_datagram(const char* data, size_t n, int& type, string& topic, string& payload){
    if(n < sizeof(Header)) return false;
    Header h{};
    memcpy(&h, data, sizeof(h));
    type = ntohl(h.type);
    int tlen = ntohl(h.topic_len);
    int plen = ntohl(h.payload_len);
    size_t need = sizeof(h) + (size_t)tlen + (size_t)plen;
    if(n < need) return false;
    topic.assign(data + sizeof(h), (size_t)tlen);
    payload.assign(data + sizeof(h) + tlen, (size_t)plen);
    return true;
}

// With timeout; fills from address if needed
static bool recv_packet_udp(int ufd, int& type, string& topic, string& payload, sockaddr_in* from_opt=nullptr){
    static vector<char> buf(64*1024);
    sockaddr_in from{}; socklen_t flen=sizeof(from);
    ssize_t n = recvfrom(ufd, buf.data(), buf.size(), 0, (sockaddr*)&from, &flen);
    if(n < 0){
        if(errno==EINTR) return false;
        return false;
    }
    if(from_opt) *from_opt = from;
    return parse_datagram(buf.data(), (size_t)n, type, topic, payload);
}

// Base64 encoding
static const char* B64="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
static unsigned char REV[256];
static void init_rev(){
    for(int i=0;i<256;++i) REV[i]=255;
    for(int i=0;i<64;++i) REV[(unsigned char)B64[i]]=(unsigned char)i;
    REV[(unsigned char)'=']=254;
}
static string b64_encode(const string& in){
    const unsigned char* d=(const unsigned char*)in.data();
    size_t n=in.size(); string out; out.reserve(((n+2)/3)*4);
    for(size_t i=0;i<n;i+=3){
        unsigned int v = d[i]<<16;
        if(i+1<n) v |= d[i+1]<<8;
        if(i+2<n) v |= d[i+2];
        out.push_back(B64[(v>>18)&63]);
        out.push_back(B64[(v>>12)&63]);
        out.push_back(i+1<n ? B64[(v>>6)&63] : '=');
        out.push_back(i+2<n ? B64[v&63]     : '=');
    }
    return out;
}
static bool b64_decode(const string& in, string& out){
    static bool inited=false; if(!inited){ init_rev(); inited=true; }
    if(in.size()%4) return false;
    out.clear(); out.reserve((in.size()/4)*3);
    for(size_t i=0;i<in.size(); i+=4){
        int c0=REV[(unsigned char)in[i]];
        int c1=REV[(unsigned char)in[i+1]];
        int c2=REV[(unsigned char)in[i+2]];
        int c3=REV[(unsigned char)in[i+3]];
        if(c0==255 || c1==255 || c2==255 || c3==255) return false;
        unsigned int v = ((unsigned int)c0<<18) | ((unsigned int)c1<<12);
        out.push_back((char)((v>>16)&0xFF));
        if(c2!=254){
            v |= (unsigned int)c2<<6;
            out.push_back((char)((v>>8)&0xFF));
            if(c3!=254){
                v |= (unsigned int)c3;
                out.push_back((char)(v&0xFF));
            }
        }
    }
    return true;
}

// Pretty addr 
static string addr_str(const sockaddr_in& a){
    char ip[INET_ADDRSTRLEN]{};
    inet_ntop(AF_INET, &a.sin_addr, ip, sizeof(ip));
    return string(ip) + ":" + to_string((int)ntohs(a.sin_port));
}

// Main 
int main(int argc, char** argv){
    if(argc < 6){
        cerr<<"Usage:\n"
            <<"  Subscriber (multi-topic): ./client <server_ip> <port> <tcp|udp> sub <topic1> [topic2 ...]\n"
            <<"  Publisher (single topic): ./client <server_ip> <port> <tcp|udp> pub <topic>\n";
        return 1;
    }
    string ip=argv[1]; int port=stoi(argv[2]); string transport=argv[3]; string role=argv[4];
    bool use_udp = (transport=="udp");
    bool is_sub  = (role=="sub");
    bool is_pub  = (role=="pub");
    if(!is_sub && !is_pub){ cerr<<"Role must be 'sub' or 'pub'\n"; return 1; }
    if(transport!="tcp" && transport!="udp"){ cerr<<"Transport must be 'tcp' or 'udp'\n"; return 1; }

    // Common server sockaddr
    sockaddr_in srv{}; srv.sin_family=AF_INET; srv.sin_port=htons(port);
    if(inet_pton(AF_INET, ip.c_str(), &srv.sin_addr)!=1){ cerr<<"Invalid IP\n"; return 1; }

    cout<<"[CLIENT] Transport="<< (use_udp ? "UDP" : "TCP")
        <<", Role="<<(is_sub?"Subscriber":"Publisher")<<", Server="<<ip<<":"<<port<<"\n";

    // Create sockets
    int fd=-1;
    if(use_udp){
        fd = socket(AF_INET, SOCK_DGRAM, 0);
        if(fd<0){ perror("socket UDP"); return 1; }

        // Set a modest recv timeout so we don't block forever when waiting for ACKs
        timeval tv{2,0}; // 2 seconds
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    } else {
        fd = socket(AF_INET, SOCK_STREAM, 0);
        if(fd<0){ perror("socket TCP"); return 1; }
        if(connect(fd,(sockaddr*)&srv,sizeof(srv))<0){ perror("connect"); return 1; }
        cout<<"[TCP] Connected to "<<ip<<":"<<port<<"\n";
    }

    // Subscriber path
    if(is_sub){
        vector<string> topics; for(int i=5;i<argc;++i) topics.push_back(argv[i]);
        if(topics.empty()){ cerr<<"Subscriber requires at least one topic\n"; return 1; }

        // Send SUBSCRIBE for each topic and wait for ACK
        for(const auto& t: topics){
            bool ok=false;
            if(use_udp){
                if(!send_packet_udp(fd, srv, SUBSCRIBE, t, "")){
                    cerr<<"[ERROR] UDP send SUBSCRIBE '"<<t<<"' failed\n"; return 1;
                }
                // Because UDP can reorder, we may receive MSG first; loop until ACK arrives (or timeout overall)
                auto start = chrono::steady_clock::now();
                while(true){
                    int ty; string tp, pl;
                    sockaddr_in from{};
                    if(!recv_packet_udp(fd, ty, tp, pl, &from)){
                        // timeout tick — check total wait
                        if(chrono::steady_clock::now() - start > chrono::seconds(5)){
                            cerr<<"[WARN] No ACK for SUBSCRIBE '"<<t<<"' within 5s (continuing to listen)\n";
                            break;
                        }
                        continue;
                    }
                    if(ty==ACK){
                        cout<<"[ACK] SUBSCRIBE confirmed for '"<<t<<"' via UDP\n";
                        ok=true; break;
                    }else if(ty==MSG){
                        string decoded; bool dec=b64_decode(pl, decoded);
                        cout<<"[RECEIVED] Topic='"<<tp<<"' base64="<<pl<<" | text="<<(dec?decoded:"<b64-decode-error>")<<"\n";
                        // keep waiting for ACK
                    } // ignore others
                }
            }else{
                if(!send_packet_tcp(fd, SUBSCRIBE, t, "")){ cerr<<"[ERROR] TCP send SUBSCRIBE failed\n"; return 1; }
                int ty; string tp, pl;
                if(!recv_packet_tcp(fd, ty, tp, pl) || ty!=ACK){ cerr<<"[ERROR] No ACK for SUBSCRIBE '"<<t<<"'\n"; return 1; }
                cout<<"[ACK] SUBSCRIBE confirmed for '"<<t<<"' via TCP\n";
                ok=true;
            }
            (void)ok; // informational; we proceed to receive anyway
        }

        cout<<"[READY] Subscribed to "<<topics.size()<<" topic(s). Waiting for messages...\n";

        // Receive loop
        if(use_udp){
            while(true){
                int ty; string tp, pl;
                if(!recv_packet_udp(fd, ty, tp, pl, nullptr)){
                    // timeout just means no packets recently; continue listening
                    continue;
                }
                if(ty==MSG){
                    string decoded; bool ok=b64_decode(pl, decoded);
                    cout<<"[RECEIVED] Topic='"<<tp<<"' base64="<<pl<<" | text="<<(ok?decoded:"<b64-decode-error>")<<"\n";
                }else if(ty==ACK){
                    // unsolicited ACK (e.g., from server after a prior action)
                    cout<<"[ACK] (unsolicited UDP)\n";
                }
            }
        }else{
            while(true){
                int ty; string tp, pl;
                if(!recv_packet_tcp(fd, ty, tp, pl)){ cerr<<"[INFO] Server closed connection.\n"; break; }
                if(ty==MSG){
                    string decoded; bool ok=b64_decode(pl, decoded);
                    cout<<"[RECEIVED] Topic='"<<tp<<"' base64="<<pl<<" | text="<<(ok?decoded:"<b64-decode-error>")<<"\n";
                }else if(ty==ACK){
                    cout<<"[ACK] (unsolicited TCP)\n";
                }
            }
        }
        // never reached in normal sub mode; Ctrl+C to quit
        ::close(fd);
        return 0;
    }

    // Publisher path
    if(is_pub){
        if(argc != 6){ cerr<<"Publisher requires exactly one topic\n"; return 1; }
        string topic=argv[5];
        cout<<"[PUBLISHER READY] Topic='"<<topic<<"'. Type messages.\n";
        string line;
        while(getline(cin, line)){
            string enc = b64_encode(line);
            bool sent=false, got_ack=false;

            if(use_udp){
                sent = send_packet_udp(fd, srv, PUBLISH, topic, enc);
                if(!sent){ cerr<<"[ERROR] UDP send PUBLISH failed\n"; break; }
                // Wait briefly for ACK (not guaranteed with UDP)
                auto start = chrono::steady_clock::now();
                while(true){
                    int ty; string tp, pl;
                    if(!recv_packet_udp(fd, ty, tp, pl, nullptr)){
                        if(chrono::steady_clock::now() - start > chrono::seconds(3)){
                            cerr<<"[WARN] No ACK for PUBLISH (UDP). Continuing.\n";
                            break;
                        }
                        continue;
                    }
                    if(ty==ACK){ got_ack=true; break; }
                    // If a MSG arrives here, we're a publisher, so ignore.
                }
            }else{
                sent = send_packet_tcp(fd, PUBLISH, topic, enc);
                if(!sent){ cerr<<"[ERROR] TCP send PUBLISH failed\n"; break; }
                int ty; string tp, pl;
                if(recv_packet_tcp(fd, ty, tp, pl) && ty==ACK) got_ack=true;
            }

            if(got_ack) cout<<"[ACK] PUBLISH confirmed (sent base64="<<enc<<") via "<<(use_udp?"UDP":"TCP")<<"\n";
            else        cout<<"[INFO] PUBLISH sent; ACK not confirmed ("<<(use_udp?"UDP":"TCP")<<")\n";
        }

        // graceful TERM
        if(use_udp){
            (void)send_packet_udp(fd, srv, TERM, topic, "");
            // try to read an ACK briefly
            auto start = chrono::steady_clock::now();
            while(true){
                int ty; string tp, pl;
                if(!recv_packet_udp(fd, ty, tp, pl, nullptr)){
                    if(chrono::steady_clock::now() - start > chrono::seconds(2)) break;
                    continue;
                }
                if(ty==ACK){
                    cout<<"[ACK] TERM confirmed via UDP\n";
                    break;
                }
            }
        }else{
            if(send_packet_tcp(fd, TERM, topic, "")){
                int ty; string tp, pl;
                if(recv_packet_tcp(fd, ty, tp, pl) && ty==ACK) cout<<"[ACK] TERM confirmed via TCP\n";
            }
        }

        ::close(fd);
        return 0;
    }

    return 0;
}
