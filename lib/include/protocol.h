#include <cstdint>

/* Values used for protocol ID and type. */
#define POLI_PROTOCOL_ID 42

/* Segment types */
#define TYPE_DATA    0   /* Data segment (uses poli_tcp_data_hdr)            */
#define TYPE_ACK     1   /* Cumulative ACK for data (uses poli_tcp_ctrl_hdr) */
#define TYPE_SYN     2   /* Handshake step 1: client -> server               */
#define TYPE_SYNACK  3   /* Handshake step 2: server -> client (payload=port)*/
#define TYPE_ACKCONN 4   /* Handshake step 3: client -> server               */

/* Header for Data segments. Must be used in your implementation as it is. */
struct __attribute__((packed)) poli_tcp_data_hdr {
    uint8_t protocol_id;
    uint8_t conn_id;
    uint8_t type;
    uint16_t seq_num;
    uint16_t len;
};

/* Header for Control segments. Must be used in your implementation as it is. */
struct __attribute__((packed)) poli_tcp_ctrl_hdr {
    uint8_t protocol_id;
    uint8_t conn_id;
    uint8_t type;
    uint16_t ack_num;
    uint16_t recv_window;
};