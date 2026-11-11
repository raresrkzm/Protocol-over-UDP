#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <cstring>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include <cassert>
#include <stdlib.h>
#include "lib.h"

#define CHUNKSIZE 1024

/* Receives a file path as a parameter and sends it to the server*/
int main(int argc, char *argv[])
{
    char buf[CHUNKSIZE];
    int bytes_sent = 0;

    /* Useful for debugging */
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);

    if (argc == 1) {
        printf("Requires filename as argument\n");
        return -1;
    }

    printf("Client starting\n");
    /* Initialize the sender thread, bandwidth 8Mb/s and 2ms delay */
    init_sender(8, 2);

    /* Connect to the server */
    struct in_addr addr;
    inet_aton("172.16.0.100", &addr);
    int conn_id = setup_connection(addr.s_addr, htons(8032));
    printf("Connected to server\n");

    /* Can be used to make the client wait a bit before sending */
    if (argc == 3){
        sleep(atoi(argv[2]));
    }

    /* Open the file */
    int fd = open(argv[1], O_RDONLY);
    assert(fd >= 0);

    // move the file pointer to the end of the file to get its size
    uint32_t size = lseek(fd, 0, SEEK_END); 

    /* We should have enough space in the sender window at the beginning*/
    send_data(conn_id, (char *)&size, sizeof(uint32_t));

    // Return at the beginning
    lseek(fd, 0, SEEK_SET);
    while (1) {

        int n = read(fd, buf, sizeof(buf));
        assert(n >= 0);

        if(n == 0)
            break;

        bytes_sent = 0;
        while (n - bytes_sent > 0) {
            /* Send to the server */
            int count = send_data(conn_id, buf + bytes_sent, n - bytes_sent);

	    if (count == -1)
		    continue;
            
            bytes_sent += count;

            /* Wait a bit before trying to send again */
            if (n - bytes_sent != 0)
                usleep(500000);
        }
    }

    printf("Finished sending the file\n");
    /* Give the other thread time to finish the transmission */
    while (1) {}

    return 0; 
}

