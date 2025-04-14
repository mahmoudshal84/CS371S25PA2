#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <pthread.h>
#include <fcntl.h>
#include <errno.h>

#define MAX_EVENTS 64
#define MESSAGE_SIZE 16
#define DEFAULT_CLIENT_THREADS 4

char *server_ip = "127.0.0.1";
int server_port = 12345;
int num_client_threads = DEFAULT_CLIENT_THREADS;
int num_requests = 1000000;

/*
 * This structure is used to store per-thread data in the client
 */
typedef struct {
    int epoll_fd;        /* File descriptor for the epoll instance, used for monitoring events on the socket. */
    int socket_fd;       /* File descriptor for the client socket connected to the server. */
    long long total_rtt; /* Accumulated Round-Trip Time (RTT) for all messages sent and received (in microseconds). */
    long total_messages; /* Total number of messages sent and received. */
    float request_rate;  /* Computed request rate (requests per second) based on RTT and total messages. */
    long tx_cnt;         /* Count of packets transmitted */
    long rx_cnt;         /* Count of packets received back */
    struct sockaddr_in server_addr; /* Server address for UDP communication */
} client_thread_data_t;

/*
 * This function runs in a separate client thread to handle communication with the server
 */
void *client_thread_func(void *arg) {
    client_thread_data_t *data = (client_thread_data_t *)arg;
    struct epoll_event event, events[MAX_EVENTS];
    char send_buf[MESSAGE_SIZE] = "ABCDEFGHIJKMLNOP"; /* Send 16-Bytes message every time */
    char recv_buf[MESSAGE_SIZE];
    struct timeval start, end;
    socklen_t addr_len = sizeof(data->server_addr);

    // Add the socket to epoll instance for monitoring
    event.events = EPOLLIN;
    event.data.fd = data->socket_fd;
    
    epoll_ctl(data->epoll_fd, EPOLL_CTL_ADD, data->socket_fd, &event);
    
    // Initialize tracking variables
    data->total_rtt = 0;
    data->total_messages = 0;
    data->tx_cnt = 0;
    data->rx_cnt = 0;
    
    // Main loop for sending messages
    for (int i = 0; i < num_requests; i++) {
        // Start timing
        gettimeofday(&start, NULL);
        
        // Send the message using UDP sendto
        int sent = sendto(data->socket_fd, send_buf, MESSAGE_SIZE, 0, 
                 (struct sockaddr*)&data->server_addr, sizeof(data->server_addr));
        if (sent < 0) {
            perror("sendto");
            continue;
        }
        data->tx_cnt++; // Increment transmitted packet counter
        
        // Wait for response
        int ready = epoll_wait(data->epoll_fd, events, MAX_EVENTS, 3000);
        if (ready < 0) {
            if (errno != EINTR) {
                printf("epoll failed: %s\n", strerror(errno));
                break;
            }
            continue; // retry on interruption
        }
        
        if (ready == 0) {
            continue; // timeout, no response received
        }
        
        // Read response using UDP recvfrom
        int n = recvfrom(data->socket_fd, recv_buf, MESSAGE_SIZE, 0, 
                 (struct sockaddr*)&data->server_addr, &addr_len);
        if (n <= 0) {
            if (n < 0) perror("recvfrom");
            continue;
        }
        
        data->rx_cnt++; // Increment received packet counter
        
        // Record time and do calculations
        gettimeofday(&end, NULL);
        
        long long seconds_diff = end.tv_sec - start.tv_sec;
        long long micros_diff = end.tv_usec - start.tv_usec;
        long long rtt = seconds_diff * 1000000 + micros_diff;
        
        // Accumulate stats
        data->total_rtt += rtt;
        data->total_messages++;
        
        if (i == 0 || i == 100 || i == 1000 || i == 10000 || i == 100000 || i == num_requests-1) {
            printf("Thread #%d: %d msgs, last RTT: %lld us\n", 
                   data->socket_fd % 100, i+1, rtt);
        }
    }
    
    // Calculate rate
    if (data->total_messages > 0) {
        double seconds = data->total_rtt / 1000000.0;
        data->request_rate = data->total_messages / seconds;
    } else {
        data->request_rate = 0;
    }

    return NULL;
}

/*
 * This function orchestrates multiple client threads to send requests to a server,
 * collect performance data of each threads, and compute aggregated metrics of all threads.
 */
void run_client() {
    pthread_t threads[num_client_threads];
    client_thread_data_t thread_data[num_client_threads];
    struct sockaddr_in server_addr;
    long long total_rtt = 0;
    long total_messages = 0;
    float total_request_rate = 0.0;
    long total_tx = 0;
    long total_rx = 0;

    printf("Client starting with %d threads\n", num_client_threads);
    
    // Set up address structure
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);
    
    // Convert IP string to binary
    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
        printf("Bad IP address\n");
        return;
    }
    
    // Create client threads
    for (int i = 0; i < num_client_threads; i++) {
        // Create UDP socket for this thread
        int sock = socket(AF_INET, SOCK_DGRAM, 0);
        if (sock < 0) {
            perror("socket");
            return;
        }
        
        // Create epoll instance
        int epfd = epoll_create1(0);
        if (epfd == -1) {
            printf("epoll_create1 failed\n");
            close(sock);
            return;
        }
        
        // Store socket and epoll fd in thread data
        thread_data[i].socket_fd = sock;
        thread_data[i].epoll_fd = epfd;
        thread_data[i].server_addr = server_addr;
        
        printf("Thread %d ready\n", i);
    }
    
    // Start threads
    for (int i = 0; i < num_client_threads; i++) {
        int result = pthread_create(&threads[i], NULL, client_thread_func, &thread_data[i]);
        if (result != 0) {
            printf("Thread creation failed: %s\n", strerror(result));
        }
    }
    
    // Wait for threads
    for (int j = 0; j < num_client_threads; j++) {
        pthread_join(threads[j], NULL);
    }
    
    // Print stats
    printf("\n----- Results -----\n");
    printf("Thread stats:\n");
    for (int i = 0; i < num_client_threads; i++) {
        total_rtt += thread_data[i].total_rtt;
        total_messages += thread_data[i].total_messages;
        total_request_rate += thread_data[i].request_rate;
        total_tx += thread_data[i].tx_cnt;
        total_rx += thread_data[i].rx_cnt;
        
        printf("Thread %d:\n", i);
        printf("  Messages: %ld\n", thread_data[i].total_messages);
        
        if (thread_data[i].total_messages > 0) {
            printf("  Avg RTT: %.2f ms\n", (double)thread_data[i].total_rtt / 
                   (thread_data[i].total_messages * 1000.0));
            printf("  Rate: %.2f req/s\n", thread_data[i].request_rate);
            
            double avg_time = (double)thread_data[i].total_rtt / 
                            (thread_data[i].total_messages * 1000000.0);
            double prod = thread_data[i].request_rate * avg_time;
            printf("  RPS×T = %f\n", prod);
            
            // Print packet loss stats
            printf("  Packets sent: %ld, received: %ld, lost: %ld (%.2f%%)\n", 
                   thread_data[i].tx_cnt, thread_data[i].rx_cnt, 
                   thread_data[i].tx_cnt - thread_data[i].rx_cnt,
                   (float)(thread_data[i].tx_cnt - thread_data[i].rx_cnt) * 100 / thread_data[i].tx_cnt);
        }
        
        close(thread_data[i].socket_fd);
        close(thread_data[i].epoll_fd);
    }
    
    // Print overall stats
    printf("\nOverall:\n");
    printf("Average RTT: %lld us\n", total_messages > 0 ? total_rtt / total_messages : 0);
    printf("Total Request Rate: %f messages/s\n", total_request_rate);
    printf("Total Packets sent: %ld, received: %ld, lost: %ld (%.2f%%)\n", 
           total_tx, total_rx, total_tx - total_rx,
           total_tx > 0 ? (float)(total_tx - total_rx) * 100 / total_tx : 0);
}

void run_server() {
    int server_fd, epoll_fd;
    struct sockaddr_in server_addr, client_addr;
    struct epoll_event event, events[MAX_EVENTS];
    
    // Create UDP server socket
    server_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (server_fd < 0) {
        perror("socket");
        return;
    }
    
    int yes = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    
    // Configure address
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);

    if (strcmp(server_ip, "0.0.0.0") == 0) {
        server_addr.sin_addr.s_addr = INADDR_ANY;
    } else {
        inet_pton(AF_INET, server_ip, &server_addr.sin_addr);
    }
    
    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind");
        close(server_fd);
        return;
    }
    
    // Create epoll instance
    epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) {
        perror("epoll_create1");
        close(server_fd);
        return;
    }
    
    // Add server socket to epoll
    event.events = EPOLLIN;
    event.data.fd = server_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &event) == -1) {
        perror("epoll_ctl");
        close(server_fd);
        close(epoll_fd);
        return;
    }
    
    printf("Server running on %s:%d\n", server_ip, server_port);
    int client_no = 0;
    char buf[MESSAGE_SIZE];

    /* Server's run-to-completion event loop */
    while (1) {
        int n = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (n == -1) {
            perror("epoll_wait");
            break;
        }
        
        for (int i = 0; i < n; i++) {
            if (events[i].data.fd == server_fd) {
                // Handle UDP datagram
                socklen_t client_len = sizeof(client_addr);
                
                // Receive data
                int bytes = recvfrom(server_fd, buf, sizeof(buf), 0,
                              (struct sockaddr*)&client_addr, &client_len);
                
                if (bytes <= 0) {
                    if (bytes < 0)
                        perror("recvfrom");
                    continue;
                }
                
                // Print client info 
                char ip_str[INET_ADDRSTRLEN];
                inet_ntop(AF_INET, &client_addr.sin_addr, ip_str, sizeof(ip_str));
                client_no++;
                printf("Received packet #%d from %s:%d\n", client_no, ip_str, 
                       ntohs(client_addr.sin_port));
                
                // Echo back
                sendto(server_fd, buf, bytes, 0, 
                      (struct sockaddr*)&client_addr, client_len);
            }
        }
    }
    
    close(epoll_fd);
    close(server_fd);
}

int main(int argc, char *argv[]) {
    if (argc > 1 && strcmp(argv[1], "server") == 0) {
        if (argc > 2) server_ip = argv[2];
        if (argc > 3) server_port = atoi(argv[3]);

        run_server();
    } else if (argc > 1 && strcmp(argv[1], "client") == 0) {
        if (argc > 2) server_ip = argv[2];
        if (argc > 3) server_port = atoi(argv[3]);
        if (argc > 4) num_client_threads = atoi(argv[4]);
        if (argc > 5) num_requests = atoi(argv[5]);

        run_client();
    } else {
        printf("Usage: %s <server|client> [server_ip server_port num_client_threads num_requests]\n", argv[0]);
    }

    return 0;
}
