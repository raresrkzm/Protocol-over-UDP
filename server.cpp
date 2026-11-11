#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <cstdio>
#include <ctime>
#include "lib.h"
#include <fcntl.h>
#include <cassert>
#include <unistd.h>
#include <stdlib.h>

#define CHUNKSIZE 512

#define TICK(X)                                                                \
  struct timespec X;                                                           \
  clock_gettime(CLOCK_MONOTONIC_RAW, &X)

#define TOCK(X)                                                                \
  struct timespec X##_end;                                                     \
  clock_gettime(CLOCK_MONOTONIC_RAW, &X##_end);                                \
  printf("Total time = %f seconds\n",                                          \
         (X##_end.tv_nsec - (X).tv_nsec) / 1000000000.0 +                      \
             (X##_end.tv_sec - (X).tv_sec))


int main(int argc, char *argv[])
{
    int len;
    
	/* Useful for debugging */
    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0);

    /* Initialize the receiver thread */

    /* 9 KB  buffer */
    init_receiver(9 * 1024);

	/* Only one client */
    if (argc == 1) {

    	printf("Server starting with one client\n");
	    /* Only one client */
    	int conn_id, fd;
    	char buf[CHUNKSIZE];
	    /* Wait for connection */
	    conn_id = wait4connect(INADDR_ANY, htons(8032));
	    printf("Accepted connection from client\n");

	    fd = open("file.out", O_CREAT|O_WRONLY|O_TRUNC, 0660);
	    assert(fd >= 0);

		uint32_t file_size;

		int i = 0;
		do {
			i += recv_data(conn_id, (char *)&file_size + i, sizeof(uint32_t) - i);
		} while (i < (int)sizeof(uint32_t));

    	TICK(TIME_A);
	    do {
			len = recv_data(conn_id, buf, CHUNKSIZE);

			file_size -= len;

			write(fd, buf, len);
			/* Files have an # at the end */
	    } while(file_size > 0);

		TOCK(TIME_A);
	    close(fd);

	/* Multiple clients. */
    } else {
    	printf("Server starting with multiple clients\n");
	    int num_con = 1;
	    int conn_ids[32];
	    int file_fd[32];
		/* Used to keep track files that got transmitted */
		int finished[32] = {0};
	    num_con = atoi(argv[1]);
		char buf[CHUNKSIZE];

		/* Accept the connections and open the files */
		for (int i = 0; i < num_con; i++) {
			char buf[64];
			sprintf(buf, "file%d.out", i);	

	    	conn_ids[i] = wait4connect(INADDR_ANY, htons(8032));
	    	file_fd[i] = open(buf, O_CREAT|O_WRONLY|O_TRUNC, 0660);
			assert(file_fd[i] > 0);
			printf("Accepted connection for client %d, writing to file %s\n", i, buf);
		}

		/* Wait a bit before strating to read data */
		sleep(2);

		uint32_t file_sizes[num_con];
		/* Read the file sizes */
		for (int i = 0; i < num_con; i++) {
			int j = 0;
			do {
				j += recv_data(conn_ids[i], (char *)&file_sizes[i] + j, sizeof(uint32_t) - j);
			} while (j < (int)sizeof(uint32_t));
		}

    	TICK(TIME_A);
		int ok = 0;
	    do {
			/* Iterate through each connection and read some data */
			for (int i = 0; i < num_con; i++) {

				if (finished[i] == 0) {
					len = recv_data(conn_ids[i], buf, CHUNKSIZE);

					file_sizes[i] -= len;
					int n = write(file_fd[i], buf, len);
					assert(n >= 0);

					if (file_sizes[i] <= 0) {
						finished[i] = 1;
						ok++;
					}
				}
				/* Files have an # at the end */
			}
	    } while(ok < num_con);

    	TOCK(TIME_A);
		for (int i = 0; i < num_con; i++) {
			close(file_fd[i]);
		}
    }

    printf("Server exiting\n");
}

