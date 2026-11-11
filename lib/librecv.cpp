#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <poll.h>
#include <cassert>
#include <sys/timerfd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <sys/time.h>
#include <algorithm>
#include <new>

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

/* Server-side globals */
static int g_listen_sockfd = -1;
static int g_recv_buffer_bytes = 0;
static int g_next_conn_id = 0;
static pthread_mutex_t g_cons_mtx = PTHREAD_MUTEX_INITIALIZER;

static void boost_socket_buffers(int sockfd)
{
    int rcvbuf = 1 << 20;
    int sndbuf = 1 << 20;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
    setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
}

/* Send a cumulative ACK on c->sockfd. Caller holds con_lock */
static void tx_ack(struct connection *c)
{
    char buf[sizeof(poli_tcp_ctrl_hdr)];
    poli_tcp_ctrl_hdr *h = (poli_tcp_ctrl_hdr *) buf;
    h->protocol_id = POLI_PROTOCOL_ID;
    h->conn_id = (uint8_t) c->conn_id;
    h->type = TYPE_ACK;
    h->ack_num = htons(c->expected_seq);

    int free_space = c->recv_buf_max - (int)c->recv_buf.size();
    if (free_space < 0)
        free_space = 0;

    if (free_space > 65535)
        free_space = 65535;

    h->recv_window = htons((uint16_t) free_space);

    sendto(c->sockfd, buf, sizeof(*h), 0,
           (struct sockaddr *)&c->servaddr, sizeof(c->servaddr));
}

/* After accepting an in-order segment, drain contiguous OOS entries */
static void drain_oos(struct connection *c)
{
    while (true) {
        auto it = c->oos.find(c->expected_seq);
        if (it == c->oos.end())
            break;

        for (char b : it->second) {
            c->recv_buf.push_back(b);
        }

        c->expected_seq++;
        c->oos.erase(it);
    }
}

/* Is seq_num inside [expected_seq, expected_seq + W) ?
 * W = recv_buf_max / MAX_DATA_SIZE = explicit receiver window
 * Uses uint16_t arithmetic so it works across wraparound */
static bool in_receiver_window(struct connection *c, uint16_t seq_num)
{
    uint16_t W = (uint16_t)(c->recv_buf_max / MAX_DATA_SIZE);
    if (W == 0)
        W = 1;

    uint16_t diff = (uint16_t)(seq_num - c->expected_seq);

    return diff < W;
}

int recv_data(int conn_id, char *buffer, int len)
{
    if (cons.find(conn_id) == cons.end()) return -1;
    struct connection *c = cons[conn_id];

    pthread_mutex_lock(&c->con_lock);

    while (c->recv_buf.empty()) {
        pthread_cond_wait(&c->data_available, &c->con_lock);
    }

    int n = std::min(len, (int)c->recv_buf.size());
    for (int i = 0; i < n; i++) {
        buffer[i] = c->recv_buf.front();
        c->recv_buf.pop_front();
    }

    pthread_mutex_unlock(&c->con_lock);
    return n;
}

void *receiver_handler(void *arg)
{
    char segment[MAX_SEGMENT_SIZE];
    int res;
    DEBUG_PRINT("Starting receiver handler\n");

    while (1) {

        int conn_id = -1;
        do {
            res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);
            if (res == -14) usleep(100);
        } while(res == -14);

        if (cons.find(conn_id) == cons.end()) continue;
        struct connection *c = cons[conn_id];

        pthread_mutex_lock(&c->con_lock);

        if (res == -1) {
            /* Timer tick at receiver: nothing to do (no retransmits here) */
        } else if (res >= (int) sizeof(poli_tcp_data_hdr)) {
            poli_tcp_data_hdr *h = (poli_tcp_data_hdr *) segment;

            if (h->protocol_id == POLI_PROTOCOL_ID && h->type == TYPE_DATA) {
                uint16_t seq_num = ntohs(h->seq_num);
                uint16_t plen = ntohs(h->len);
                char *payload = segment + sizeof(*h);

                /* Receiver Sliding Window
                 *   accept_window = [expected_seq, expected_seq + W)      *
                 *   W = recv_buf_max / MAX_DATA_SIZE                      *
                 */
                if (!in_receiver_window(c, seq_num)) {
                    /* Outside window: drop without buffering. Just re-ACK
                     * so a duplicate retransmit doesn't sit unacknowledged. */
                    tx_ack(c);
                } else if (seq_num == c->expected_seq) {
                    /* In-order: append to recv_buf */
                    int free_space = c->recv_buf_max - (int) c->recv_buf.size();
                    if ((int) plen <= free_space) {
                        for (int i = 0; i < (int)plen; i++) {
                            c->recv_buf.push_back(payload[i]);
                        }

                        c->expected_seq++;
                        drain_oos(c);
                        pthread_cond_signal(&c->data_available);
                    } else {
                        DEBUG_PRINT("Receiver: drop in-order seq=%u (no space)\n", seq_num);
                    }
                    tx_ack(c);
                } else {
                    /* In window but ahead: keep for later (selective) */
                    if (c->oos.find(seq_num) == c->oos.end()) {
                        c->oos[seq_num] = std::vector<char>(payload, payload + plen);
                    }
                    /* Duplicate ACK with same expected_seq -> drives
                     * sender's fast retransmit for the gap */
                    tx_ack(c);
                }
            }
        }

        pthread_mutex_unlock(&c->con_lock);
    }
}

int wait4connect(uint32_t ip, uint16_t port)
{
    while (1) {
        char rxbuf[64];
        struct sockaddr_in client;
        socklen_t clientlen = sizeof(client);

        ssize_t n = recvfrom(g_listen_sockfd, rxbuf, sizeof(rxbuf), 0,
                             (struct sockaddr *)&client, &clientlen);
        if (n < (ssize_t) sizeof(poli_tcp_ctrl_hdr)) continue;

        poli_tcp_ctrl_hdr *syn = (poli_tcp_ctrl_hdr *)rxbuf;
        if (syn->protocol_id != POLI_PROTOCOL_ID || syn->type != TYPE_SYN)
            continue;

        DEBUG_PRINT("Server: got SYN from %s:%d\n",
                    inet_ntoa(client.sin_addr), ntohs(client.sin_port));

        pthread_mutex_lock(&g_cons_mtx);
        int new_conn_id = g_next_conn_id++;
        pthread_mutex_unlock(&g_cons_mtx);

        int new_sock = socket(AF_INET, SOCK_DGRAM, 0);
        assert(new_sock >= 0);
        boost_socket_buffers(new_sock);

        struct sockaddr_in srvaddr;
        memset(&srvaddr, 0, sizeof(srvaddr));
        srvaddr.sin_family = AF_INET;
        srvaddr.sin_addr.s_addr = INADDR_ANY;
        srvaddr.sin_port = 0;
        int br = bind(new_sock, (struct sockaddr *)&srvaddr, sizeof(srvaddr));
        assert(br == 0);

        struct sockaddr_in actual;
        socklen_t actuallen = sizeof(actual);
        getsockname(new_sock, (struct sockaddr *)&actual, &actuallen);
        uint16_t new_port_host = ntohs(actual.sin_port);

        DEBUG_PRINT("Server: alloc conn_id=%d, data port=%d\n",
                    new_conn_id, new_port_host);

        char sabuf[sizeof(poli_tcp_ctrl_hdr) + 2];
        poli_tcp_ctrl_hdr *sa = (poli_tcp_ctrl_hdr *) sabuf;
        sa->protocol_id = POLI_PROTOCOL_ID;
        sa->conn_id = (uint8_t) new_conn_id;
        sa->type = TYPE_SYNACK;
        sa->ack_num = 0;
        sa->recv_window = htons((uint16_t)g_recv_buffer_bytes);
        uint16_t net_port = htons(new_port_host);
        memcpy(sabuf + sizeof(*sa), &net_port, sizeof(net_port));

        struct timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        setsockopt(new_sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        bool established = false;
        for (int attempt = 0; attempt < 10; attempt++) {
            sendto(new_sock, sabuf, sizeof(sabuf), 0,
                   (struct sockaddr *)&client, sizeof(client));
            DEBUG_PRINT("Server: sent SYN-ACK (attempt %d)\n", attempt);

            char abuf[MAX_SEGMENT_SIZE];
            struct sockaddr_in from;
            socklen_t fromlen = sizeof(from);
            ssize_t an = recvfrom(new_sock, abuf, sizeof(abuf), 0,
                                  (struct sockaddr *)&from, &fromlen);

            if (an < 0)
                continue;
            if (an < (ssize_t)sizeof(poli_tcp_ctrl_hdr))
                continue;

            poli_tcp_ctrl_hdr *a = (poli_tcp_ctrl_hdr *) abuf;
            if (a->protocol_id != POLI_PROTOCOL_ID)
                continue;

            if (a->conn_id == (uint8_t) new_conn_id &&
                (a->type == TYPE_ACKCONN || a->type == TYPE_DATA)) {
                established = true;
                break;
            }
        }

        if (!established) {
            DEBUG_PRINT("Server: handshake timed out, abandoning conn_id=%d\n", new_conn_id);
            close(new_sock);
            continue;
        }

        struct timeval tv0 = {0, 0};
        setsockopt(new_sock, SOL_SOCKET, SO_RCVTIMEO, &tv0, sizeof(tv0));

        struct connection *con = (struct connection *)malloc(sizeof(struct connection));
        new (con) connection();

        con->sockfd = new_sock;
        con->conn_id = new_conn_id;
        con->servaddr = client;
        con->max_window_seq = 16;       /* unused on receiver */
        con->recv_buf_max = g_recv_buffer_bytes;
        con->expected_seq = 0;
        pthread_mutex_init(&con->con_lock, NULL);
        pthread_cond_init(&con->data_available, NULL);

        pthread_mutex_lock(&g_cons_mtx);

        data_fds[fdmax].fd = new_sock;
        data_fds[fdmax].events = POLLIN;

        timer_fds[fdmax].fd = timerfd_create(CLOCK_MONOTONIC, 0);
        timer_fds[fdmax].events = POLLIN;
        struct itimerspec spec;
        spec.it_value.tv_sec = 1;
        spec.it_value.tv_nsec = 0;
        spec.it_interval.tv_sec = 1;
        spec.it_interval.tv_nsec = 0;
        timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);

        con->timer_idx = fdmax;
        fdmax++;

        cons.insert({new_conn_id, con});

        pthread_mutex_unlock(&g_cons_mtx);

        DEBUG_PRINT("Server: connection established conn_id=%d (recv_window=%d B)\n",
                    new_conn_id, g_recv_buffer_bytes);
        return new_conn_id;
    }
}

void init_receiver(int recv_buffer_bytes)
{
    pthread_t thread1;
    int ret;

    g_recv_buffer_bytes = recv_buffer_bytes;

    g_listen_sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    assert(g_listen_sockfd >= 0);
    boost_socket_buffers(g_listen_sockfd);

    int reuse = 1;
    setsockopt(g_listen_sockfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(8032);

    if (bind(g_listen_sockfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind listen socket");
        exit(1);
    }

    DEBUG_PRINT("Server: listening on port 8032 (recv_buf=%d)\n", recv_buffer_bytes);

    ret = pthread_create(&thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);
}