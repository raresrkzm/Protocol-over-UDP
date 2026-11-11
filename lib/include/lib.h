#pragma once

#include <cstdint>
#include "utils.h"
#include <arpa/inet.h>
#include <pthread.h>
#include <deque>
#include <map>
#include <vector>

/* Maximum segment size, change as you see fit */
#define MAX_DATA_SIZE 1024
#define MAX_SEGMENT_SIZE (MAX_DATA_SIZE + sizeof(poli_tcp_data_hdr))

#define MAX_CONNECTIONS 32

/* Send buffer (per connection): bytes from app waiting to be segmented
 * Big enough to hold an entire test file so client.cpp's send_data never
 * partially-accepts (which would trigger its usleep(500000) penalty) */
#define SEND_BUF_CAP (16 * 1024 * 1024)

/* Bytes-on-wire we want to keep in flight on this link.
 * BDP = (speed_bps / 8) * RTT_sec.  For speed=8Mb/s, RTT=4ms => 4000 bytes
 * It use ~3x BDP for headroom against jitter + loss recovery, with a floor */
#define MIN_WINDOW_SEGS 16

/* A single in-flight segment kept in the sender window */
struct sent_segment {
    uint16_t seq_num;
    uint16_t len;
    int64_t  first_sent_ms;   /* monotonic ms when first sent or last retransmitted */
    char payload[MAX_DATA_SIZE];
};

/* Protocol control block. Used track different parameters about a connection
 * Will need to be extenden to solve the homework with other parameters such as
 * last_ack or status depending on how you implement your protocol */
struct connection {
    int sockfd;                  /* socket used for this connection */
    int conn_id;                 /* connection identifier */
    struct sockaddr_in servaddr; /* used to identify the destination */
    pthread_mutex_t con_lock;    /* used for syncronization with the handler thread and
                                    read/send calls.*/

    /* Index in data_fds[] / timer_fds[] */
    int timer_idx;

    /* Parameters used only by the sender
     * Sliding window of in-flight segments + a holding buffer for application
     * bytes that haven't been packetized yet
     * Effective window = min(max_window_seq, peer_recv_window/MAX_DATA_SIZE).
     */
    int max_window_seq;               /* used to store the max number of packets that can be
                                         inflight, since we can have many more packets
                                         in our window */
    std::deque<char> send_buf;        /* bytes from app, not yet segmented */
    std::deque<sent_segment> window;  /* in-flight, oldest first (window.front() = send_base) */
    uint16_t next_seq_num;            /* next seq to allocate */
    uint16_t peer_recv_window;        /* last advertised by receiver (bytes) */
    uint16_t last_ack_num;            /* for duplicate ACK detection */
    int dup_ack_count;                /* counts duplicates of last_ack_num */

    /* Parameters used only by the receiver
     * Sliding window of acceptable seq numbers: [expected_seq, expected_seq + W),
     * where W = recv_buf_max / MAX_DATA_SIZE. Segments outside are discarded.
     * In-window segments either go to recv_buf (in-order) or oos (out-of-order).
     */
    std::deque<char> recv_buf;                      /* ordered bytes for recv_data */
    int recv_buf_max;                               /* advertised window capacity (bytes) */
    std::map<uint16_t, std::vector<char>> oos;      /* out-of-order segments inside window */
    uint16_t expected_seq;                          /* left edge of receive window */
    pthread_cond_t data_available;                  /* signals blocking recv_data */
};

/* ########## API that we expose to the application ########### */

/* Equivalent of listen. Ran by the server to waits for a connection from a
 * client. Returns a connection id. Blocking untill it receives a connection
 * request */
int wait4connect(uint32_t ip, uint16_t port);
/* Equivalent of connect. Used by the client to connect to a server. */
int setup_connection(uint32_t ip, uint16_t port);
/* Equivalent to recv. Blocking if there is no data to be written in buffer */
int recv_data(int connectionid, char *buffer, int len);
/* Equivalent to send. Used by the client to send a stream of bytes as segments */
int send_data(int conn_id, char *buffer, int len);
/* Used to initialize your protocol on the receiver side. */
void init_receiver(int recv_buffer_bytes);
/* Used to initialize your protocol on the sender side */
void init_sender(int speed, int delay);

/* ######### Internal API used by sender and receiver ########### */
int recv_message_or_timeout(char *buff, size_t len, int *conn_id);