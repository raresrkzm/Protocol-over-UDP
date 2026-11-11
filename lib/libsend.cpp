#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <cassert>
#include <poll.h>
#include <sys/timerfd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <sys/time.h>
#include <time.h>
#include <algorithm>
#include <new>

using namespace std;

std::map<int, struct connection *> cons;

struct pollfd data_fds[MAX_CONNECTIONS];
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;

/* Globals set by init_sender */
static int g_speed = 0;
static int g_delay = 0;
static int g_max_window = MIN_WINDOW_SEGS;
static int g_rto_ms = 31;         /* retransmit timeout per segment */
static int g_timer_tick_ms = 15;  /* how often the timerfd fires */

static int64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}

/* Boost kernel socket buffers so bursts don't get dropped */
static void boost_socket_buffers(int sockfd)
{
    int rcvbuf = 1 << 20;   /* 1 MB */
    int sndbuf = 1 << 20;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
    setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
}

/* Build + send a DATA segment on conn's socket. Caller holds con_lock.
 * Does NOT touch first_sent_ms — callers do that depending on whether
 * this is a first send or a retransmit */
static void tx_data_segment(struct connection *c, const sent_segment &seg)
{
    char buf[sizeof(poli_tcp_data_hdr) + MAX_DATA_SIZE];
    poli_tcp_data_hdr *h = (poli_tcp_data_hdr *) buf;
    h->protocol_id = POLI_PROTOCOL_ID;
    h->conn_id = (uint8_t)c->conn_id;
    h->type = TYPE_DATA;
    h->seq_num = htons(seg.seq_num);
    h->len = htons(seg.len);
    memcpy(buf + sizeof(*h), seg.payload, seg.len);

    sendto(c->sockfd, buf, sizeof(*h) + seg.len, 0,
           (struct sockaddr *)&c->servaddr, sizeof(c->servaddr));
}

/* Effective number of segments allowed in flight RIGHT NOW */
static int allowed_in_flight(struct connection *c)
{
    int by_cwnd = c->max_window_seq;
    int by_rwnd = (int) c->peer_recv_window / MAX_DATA_SIZE;
    return std::min(by_cwnd, by_rwnd);
}

/* Pull bytes from send_buf, build a new segment, push on window, send it */
static void send_one_new_segment(struct connection *c)
{
    sent_segment seg;
    int chunk = std::min((int) MAX_DATA_SIZE, (int) c->send_buf.size());
    seg.seq_num = c->next_seq_num;
    seg.len = (uint16_t) chunk;

    for (int i = 0; i < chunk; i++) {
        seg.payload[i] = c->send_buf.front();
        c->send_buf.pop_front();
    }

    seg.first_sent_ms = now_ms();
    c->window.push_back(seg);
    c->next_seq_num++;

    tx_data_segment(c, seg);
}

/* Send as many new segments as the window = min(cwnd, rwnd) allows
 * When window + buf are both empty -> nothing
 * When window empty AND send_buf non-empty AND allowed == 0 -> single probe */
static void try_send_more(struct connection *c)
{
    int allowed = allowed_in_flight(c);

    /* Zero-window probe: avoid deadlock when peer advertises 0 and we have
     * no in-flight segment whose retransmit would refresh the window */
    if (allowed == 0 && c->window.empty() && !c->send_buf.empty()) {
        allowed = 1;
    }

    while ((int)c->window.size() < allowed && !c->send_buf.empty()) {
        send_one_new_segment(c);
    }
}

int send_data(int conn_id, char *buffer, int len)
{
    if (cons.find(conn_id) == cons.end()) return -1;
    struct connection *c = cons[conn_id];

    pthread_mutex_lock(&c->con_lock);

    int avail = SEND_BUF_CAP - (int)c->send_buf.size();
    if (avail <= 0) {
        pthread_mutex_unlock(&c->con_lock);
        return -1;
    }

    int n = std::min(len, avail);
    for (int i = 0; i < n; i++) {
        c->send_buf.push_back(buffer[i]);
    }

    /* Kick the pipeline immediately rather than waiting for the next event */
    try_send_more(c);

    pthread_mutex_unlock(&c->con_lock);
    return n;
}

void *sender_handler(void *arg)
{
    int res = 0;
    char buf[MAX_SEGMENT_SIZE];
    DEBUG_PRINT("Starting sender handler\n");

    while (1) {

        if (cons.size() == 0) {
            usleep(1000);
            continue;
        }

        int conn_id = -1;
        do {
            res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);
            if (res == -14) usleep(100);
        } while(res == -14);

        if (cons.find(conn_id) == cons.end()) continue;
        struct connection *c = cons[conn_id];

        pthread_mutex_lock(&c->con_lock);

        if (res == -1) {
            /* Timer tick: retransmit window.front() only if it's been
             * in flight longer than RTO. This avoids retransmitting
             * a segment that was sent 1-2ms ago just because the
             * periodic timer happened to fire */
            if (!c->window.empty()) {
                int64_t now = now_ms();
                sent_segment &front = c->window.front();
                if (now - front.first_sent_ms >= g_rto_ms) {
                    DEBUG_PRINT("Sender: RTO retransmit seq=%u (age=%lldms, window=%zu)\n",
                                front.seq_num,
                                (long long)(now - front.first_sent_ms),
                                c->window.size());
                    tx_data_segment(c, front);
                    front.first_sent_ms = now;
                }
            }
        } else if (res >= (int) sizeof(poli_tcp_ctrl_hdr)) {
            poli_tcp_ctrl_hdr *hdr = (poli_tcp_ctrl_hdr *) buf;
            if (hdr->protocol_id == POLI_PROTOCOL_ID && hdr->type == TYPE_ACK) {

                uint16_t ack_num = ntohs(hdr->ack_num);
                uint16_t recv_window = ntohs(hdr->recv_window);
                c->peer_recv_window = recv_window;

                /* Cumulative ACK: drop everything strictly older than ack_num */
                while (!c->window.empty() && c->window.front().seq_num < ack_num) {
                    c->window.pop_front();
                }

                /* Fast retransmit: fire exactly once per series of duplicate ACKs.
                 * Skip when the receiver advertises a tight window (it's not a
                 * loss, the receiver is just backpressuring). */
                if (ack_num == c->last_ack_num) {
                    c->dup_ack_count++;
                    if (c->dup_ack_count == 3 && !c->window.empty()
                            && c->peer_recv_window >= MAX_DATA_SIZE) {
                        DEBUG_PRINT("Sender: FAST RETRANSMIT seq=%u (3 dup-ACK %u)\n",
                                    c->window.front().seq_num, ack_num);
                        tx_data_segment(c, c->window.front());
                        c->window.front().first_sent_ms = now_ms();
                    }
                } else {
                    c->last_ack_num   = ack_num;
                    c->dup_ack_count  = 0;
                }
            }
            /* silently ignore stray non-ACK */
        }

        /* In all cases (ACK or timer), top up the window if room + data */
        try_send_more(c);

        pthread_mutex_unlock(&c->con_lock);
    }
}

int setup_connection(uint32_t ip, uint16_t port)
{
    /* Implement the sender part of the Three Way Handshake. Blocks
    until the connection is established */

    struct connection *con = (struct connection *)malloc(sizeof(struct connection));
    new (con) connection();

    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    assert(con->sockfd >= 0);
    boost_socket_buffers(con->sockfd);

    con->servaddr.sin_family = AF_INET;
    con->servaddr.sin_addr.s_addr = ip;
    con->servaddr.sin_port = port;

    struct timeval tv;
    tv.tv_sec  = 1;
    tv.tv_usec = 0;
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    int assigned_conn_id = -1;
    uint16_t initial_peer_window = 0;

    for (int attempt = 0; attempt < 20; attempt++) {

        char synbuf[sizeof(poli_tcp_ctrl_hdr)];
        poli_tcp_ctrl_hdr *syn = (poli_tcp_ctrl_hdr *)synbuf;
        syn->protocol_id = POLI_PROTOCOL_ID;
        syn->conn_id = 0;
        syn->type = TYPE_SYN;
        syn->ack_num = 0;
        syn->recv_window = 0;

        ssize_t n = sendto(con->sockfd, synbuf, sizeof(synbuf), 0,
                           (struct sockaddr *)&con->servaddr,
                           sizeof(con->servaddr));

        if (n < 0) {
            perror("sendto SYN");
            continue;
        }

        DEBUG_PRINT("Client: sent SYN (attempt %d)\n", attempt);

        char rxbuf[64];
        struct sockaddr_in from;
        socklen_t fromlen = sizeof(from);
        n = recvfrom(con->sockfd, rxbuf, sizeof(rxbuf), 0,
                     (struct sockaddr *)&from, &fromlen);

        if (n < 0) {
            DEBUG_PRINT("Client: SYN-ACK timeout\n");
            continue;
        }

        if (n < (ssize_t) (sizeof(poli_tcp_ctrl_hdr) + 2))
            continue;

        poli_tcp_ctrl_hdr *sa = (poli_tcp_ctrl_hdr *) rxbuf;
        if (sa->protocol_id != POLI_PROTOCOL_ID || sa->type != TYPE_SYNACK)
            continue;

        uint16_t net_port;
        memcpy(&net_port, rxbuf + sizeof(poli_tcp_ctrl_hdr), sizeof(net_port));
        uint16_t new_port = ntohs(net_port);
        assigned_conn_id = sa->conn_id;
        initial_peer_window = ntohs(sa->recv_window);

        DEBUG_PRINT("Client: got SYN-ACK, conn_id=%d, port=%d, peer_window=%u\n",
                    assigned_conn_id, new_port, initial_peer_window);

        con->servaddr.sin_port = htons(new_port);

        char ackbuf[sizeof(poli_tcp_ctrl_hdr)];
        poli_tcp_ctrl_hdr *ack = (poli_tcp_ctrl_hdr *) ackbuf;
        ack->protocol_id = POLI_PROTOCOL_ID;
        ack->conn_id = assigned_conn_id;
        ack->type = TYPE_ACKCONN;
        ack->ack_num = 0;
        ack->recv_window = 0;
        sendto(con->sockfd, ackbuf, sizeof(ackbuf), 0,
               (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));

        DEBUG_PRINT("Client: sent ACK, handshake complete\n");
        break;
    }

    if (assigned_conn_id < 0) {
        DEBUG_PRINT("Client: handshake failed\n");
        close(con->sockfd);
        con->~connection();
        free(con);
        return -1;
    }

    /* Disable SO_RCVTIMEO; sender_handler now drives this socket */
    struct timeval tv0 = {0, 0};
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv0, sizeof(tv0));

    con->conn_id = assigned_conn_id;
    con->max_window_seq = g_max_window;
    con->next_seq_num = 0;
    con->peer_recv_window = initial_peer_window;
    con->last_ack_num = 0;
    con->dup_ack_count = 0;

    /* Register in poll arrays */
    data_fds[fdmax].fd = con->sockfd;
    data_fds[fdmax].events = POLLIN;

    timer_fds[fdmax].fd = timerfd_create(CLOCK_MONOTONIC, 0);
    timer_fds[fdmax].events = POLLIN;
    struct itimerspec spec;
    spec.it_value.tv_sec = 0;
    spec.it_value.tv_nsec = g_timer_tick_ms * 1000000L;
    spec.it_interval.tv_sec = 0;
    spec.it_interval.tv_nsec = g_timer_tick_ms * 1000000L;
    timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);

    con->timer_idx = fdmax;
    fdmax++;

    pthread_mutex_init(&con->con_lock, NULL);
    cons.insert({assigned_conn_id, con});

    DEBUG_PRINT("Connection established! conn_id=%d (cwnd=%d segs, rto=%dms, tick=%dms)\n",
                assigned_conn_id, g_max_window, g_rto_ms, g_timer_tick_ms);

    return assigned_conn_id;
}

void init_sender(int speed, int delay)
{
    pthread_t thread1;
    int ret;

    g_speed = speed;
    g_delay = delay;

    /* Compute cwnd from BDP
     *   speed in Mb/s, delay in ms one-way (so RTT = 2 * delay ms)
     *   BDP(bytes) = (speed * 1e6 / 8) * (2 * delay / 1000)
     *              = speed * delay * 250
     *   We aim for 3x BDP in flight, never below MIN_WINDOW_SEGS
     */
    int bdp_bytes = speed * delay * 250;
    int target_bytes = 3 * bdp_bytes;
    int target_segs = target_bytes / MAX_DATA_SIZE;
    g_max_window = std::max(MIN_WINDOW_SEGS, target_segs);

    /* RTO ~ 8x RTT minimum, never below 20ms (jitter floor)
     * Timer tick ~half RTO so we don't sit idle for too long after expiry */
    int rtt_ms = 2 * delay;
    g_rto_ms = std::max(20, 8 * rtt_ms);
    g_timer_tick_ms = std::max(5, g_rto_ms / 2);

    ret = pthread_create(&thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
}