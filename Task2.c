/*
# Copyright 2025 University of Kentucky
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0
*/

/* 
Please specify the group members here
# Student #1: Mahmoud Shalash
*/

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
#include <stdbool.h>
#include <time.h>
#include <limits.h>

#define MAX_EVENTS 64
#define MESSAGE_SIZE 16
#define HEADER_SIZE 12  // Size of our custom header
#define PAYLOAD_SIZE (MESSAGE_SIZE - HEADER_SIZE)
#define DEFAULT_CLIENT_THREADS 4
#define TIMEOUT_MS 1000  // Timeout in milliseconds
#define MAX_RETRIES 5    // Maximum retransmission attempts
#define MAX_CLIENTS 1000 // Maximum number of clients the server can track

char *server_ip = "127.0.0.1";
int server_port = 12345;
int num_client_threads = DEFAULT_CLIENT_THREADS;
int num_requests = 1000000;

// Frame types
typedef enum {
    DATA = 0,
    ACK = 1,
    NAK = 2
} frame_kind_t;

// Packet header structure
typedef struct {
    frame_kind_t kind;       // Type of frame: DATA, ACK, or NAK
    uint32_t seq;            // Sequence number 
    uint32_t client_id;      // Client ID for identifying the sender
    uint32_t ack;            // Acknowledgement number
} packet_header_t;

// Full packet structure
typedef struct {
    packet_header_t header;  // Packet header
    char data[PAYLOAD_SIZE]; // Payload data
} packet_t;

/*
 * This structure is used to store per-thread data in the client
 */
typedef struct {
    int epoll_fd;            /* File descriptor for the epoll instance, used for monitoring events on the socket. */
    int socket_fd;           /* File descriptor for the client socket connected to the server. */
    long long total_rtt;     /* Accumulated Round-Trip Time (RTT) for all messages sent and received (in microseconds). */
    long total_messages;     /* Total number of messages sent and received. */
    float request_rate;      /* Computed request rate (requests per second) based on RTT and total messages. */
    long tx_cnt;             /* Count of packets transmitted (original transmissions) */
    long rx_cnt;             /* Count of packets received back */
    long retx_cnt;           /* Count of packet retransmissions */
    struct sockaddr_in server_addr; /* Server address for UDP communication */
    int client_id;           /* Client thread ID */
} client_thread_data_t;

// Server connection tracking
typedef struct {
    struct sockaddr_in addr;  // Client address
    uint32_t last_seq;        // Last sequence number processed
    time_t last_seen;         // Timestamp of last packet
    bool active;              // Whether this client is active
} client_connection_t;

// Initialize a packet with header and data
void init_packet(packet_t *pkt, frame_kind_t kind, uint32_t seq, 
                 uint32_t client_id, uint32_t ack, const char *data) {
    pkt->header.kind = kind;
    pkt->header.seq = seq;
    pkt->header.client_id = client_id;
    pkt->header.ack = ack;
    
    if (data != NULL) {
        memcpy(pkt->data, data, PAYLOAD_SIZE);
    } else {
        memset(pkt->data, 0, PAYLOAD_SIZE);
    }
}

// Add non-blocking flag to a socket
int set_socket_nonblocking(int sockfd) {
    int flags = fcntl(sockfd, F_GETFL, 0);
    if (flags == -1) {
        perror("fcntl F_GETFL");
        return -1;
    }
    
    if (fcntl(sockfd, F_SETFL, flags | O_NONBLOCK) == -1) {
        perror("fcntl F_SETFL O_NONBLOCK");
        return -1;
    }
    
    return 0;
}

/*
 * This function runs in a separate client thread to handle communication with the server
 */
void *client_thread_func(void *arg) {
    client_thread_data_t *data = (client_thread_data_t *)arg;
    struct epoll_event event, events[MAX_EVENTS];
    char payload[PAYLOAD_SIZE] = "ABCDEF"; /* 6-byte payload (adjusted for header size) */
    packet_t send_pkt, recv_pkt;
    struct timeval start, end;
    socklen_t addr_len = sizeof(data->server_addr);
    uint32_t seq_num = 0;

    // Add the socket to epoll instance for monitoring
    event.events = EPOLLIN;
    event.data.fd = data->socket_fd;
    
    if (epoll_ctl(data->epoll_fd, EPOLL_CTL_ADD, data->socket_fd, &event) == -1) {
        perror("epoll_ctl");
        return NULL;
    }
    
    // Initialize tracking variables
    data->total_rtt = 0;
    data->total_messages = 0;
    data->tx_cnt = 0;
    data->rx_cnt = 0;
    data->retx_cnt = 0;
    
    // Main loop for sending messages
    for (int i = 0; i < num_requests; i++) {
        int retries = 0;
        bool packet_acked = false;
        
        // Start timing
        gettimeofday(&start, NULL);
        
        // Prepare packet with sequence number
        init_packet(&send_pkt, DATA, seq_num, data->client_id, 0, payload);
        
        while (!packet_acked && retries < MAX_RETRIES) {
            // Send the packet
            int sent = sendto(data->socket_fd, &send_pkt, sizeof(packet_t), 0, 
                     (struct sockaddr*)&data->server_addr, sizeof(data->server_addr));
            if (sent < 0) {
                perror("sendto");
                break;
            }
            
            if (retries == 0) {
                data->tx_cnt++; // Count only original transmissions
            } else {
                data->retx_cnt++; // Count retransmissions
            }
            
            // Wait for response with timeout
            int ready = epoll_wait(data->epoll_fd, events, MAX_EVENTS, TIMEOUT_MS);
            if (ready < 0) {
                if (errno != EINTR) {
                    perror("epoll_wait");
                    break;
                }
                retries++;
                continue; // retry on interruption
            }
            
            if (ready == 0) {
                // Timeout occurred, retransmit
                printf("Thread #%d: Timeout for seq %u, retrying (%d/%d)\n", 
                       data->client_id, seq_num, retries+1, MAX_RETRIES);
                retries++;
                continue;
            }
            
            // Read response
            int n = recvfrom(data->socket_fd, &recv_pkt, sizeof(packet_t), 0, 
                     (struct sockaddr*)&data->server_addr, &addr_len);
            if (n <= 0) {
                if (n < 0) perror("recvfrom");
                retries++;
                continue;
            }
            
            // Check if received ACK matches our sequence number and client ID
            if (recv_pkt.header.kind == ACK && 
                recv_pkt.header.ack == seq_num && 
                recv_pkt.header.client_id == data->client_id) {
                
                packet_acked = true;
                data->rx_cnt++;
                
                // Calculate RTT
                gettimeofday(&end, NULL);
                long long seconds_diff = end.tv_sec - start.tv_sec;
                long long micros_diff = end.tv_usec - start.tv_usec;
                long long rtt = seconds_diff * 1000000 + micros_diff;
                
                // Accumulate stats
                data->total_rtt += rtt;
                data->total_messages++;
                
                if (i == 0 || i == 100 || i == 1000 || i == 10000 || i == 100000 || i == num_requests-1) {
                    printf("Thread #%d: %d msgs, seq %u, last RTT: %lld us\n", 
                           data->client_id, i+1, seq_num, rtt);
                }
                
                // Move to next sequence number
                seq_num = (seq_num + 1) % UINT_MAX;
            } else if (recv_pkt.header.kind == NAK) {
                // Got NAK, retry immediately
                printf("Thread #%d: Received NAK for seq %u\n", data->client_id, seq_num);
                retries++;
            } else {
                // Got wrong ACK, retry
                printf("Thread #%d: Unexpected response kind=%d ack=%u client_id=%u, expected seq=%u client_id=%d\n", 
                       data->client_id, recv_pkt.header.kind, recv_pkt.header.ack, 
                       recv_pkt.header.client_id, seq_num, data->client_id);
                retries++;
            }
        }
        
        if (!packet_acked) {
            printf("Thread #%d: Failed to get ACK for seq %u after %d attempts\n", 
                   data->client_id, seq_num, MAX_RETRIES);
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
    long total_retx = 0;

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
        
        // Set socket to non-blocking
        if (set_socket_nonblocking(sock) < 0) {
            close(sock);
            return;
        }
        
        // Create epoll instance
        int epfd = epoll_create1(0);
        if (epfd == -1) {
            perror("epoll_create1");
            close(sock);
            return;
        }
        
        // Store socket and epoll fd in thread data
        thread_data[i].socket_fd = sock;
        thread_data[i].epoll_fd = epfd;
        thread_data[i].server_addr = server_addr;
        thread_data[i].client_id = i;
        
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
        total_retx += thread_data[i].retx_cnt;
        
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
            
            // Print packet stats
            printf("  Packets sent: %ld, received: %ld, retransmitted: %ld\n", 
                   thread_data[i].tx_cnt, thread_data[i].rx_cnt, thread_data[i].retx_cnt);
            
            // Calculate effective loss rate (before retransmission)
            float loss_pct = 0;
            if (thread_data[i].tx_cnt > 0) {
                loss_pct = (float)thread_data[i].retx_cnt * 100 / thread_data[i].tx_cnt;
            }
            printf("  Effective loss rate: %.2f%%\n", loss_pct);
        }
        
        close(thread_data[i].socket_fd);
        close(thread_data[i].epoll_fd);
    }
    
    // Print overall stats
    printf("\nOverall:\n");
    printf("Average RTT: %lld us\n", total_messages > 0 ? total_rtt / total_messages : 0);
    printf("Total Request Rate: %f messages/s\n", total_request_rate);
    printf("Total Packets sent: %ld, received: %ld, retransmitted: %ld\n", 
           total_tx, total_rx, total_retx);
    
    // Calculate overall effective loss rate (before retransmission)
    float loss_pct = 0;
    if (total_tx > 0) {
        loss_pct = (float)total_retx * 100 / total_tx;
    }
    printf("Overall effective loss rate: %.2f%%\n", loss_pct);
}

// Find client in the clients array
int find_client(client_connection_t *clients, int count, struct sockaddr_in *addr) {
    for (int i = 0; i < count; i++) {
        if (clients[i].active && 
            clients[i].addr.sin_addr.s_addr == addr->sin_addr.s_addr && 
            clients[i].addr.sin_port == addr->sin_port) {
            return i;
        }
    }
    return -1;
}

// Add a new client to the clients array
int add_client(client_connection_t *clients, int *count, struct sockaddr_in *addr) {
    // First look for an inactive slot
    for (int i = 0; i < *count; i++) {
        if (!clients[i].active) {
            clients[i].addr = *addr;
            clients[i].last_seq = 0;
            clients[i].last_seen = time(NULL);
            clients[i].active = true;
            return i;
        }
    }
    
    // If no inactive slot found and we have room, add a new client
    if (*count < MAX_CLIENTS) {
        int idx = *count;
        clients[idx].addr = *addr;
        clients[idx].last_seq = 0;
        clients[idx].last_seen = time(NULL);
        clients[idx].active = true;
        (*count)++;
        return idx;
    }
    
    return -1; // No space available
}

// Server implementation
void run_server() {
    int server_fd, epoll_fd;
    struct sockaddr_in server_addr, client_addr;
    struct epoll_event event, events[MAX_EVENTS];
    client_connection_t clients[MAX_CLIENTS];
    int client_count = 0;
    int total_packets_received = 0;
    
    // Initialize clients array
    memset(clients, 0, sizeof(clients));
    
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
    
    // Set socket to non-blocking
    if (set_socket_nonblocking(server_fd) < 0) {
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
    packet_t recv_pkt, send_pkt;

    /* Server's run-to-completion event loop */
    while (1) {
        int n = epoll_wait(epoll_fd, events, MAX_EVENTS, 1000); // 1 second timeout for cleanup
        if (n == -1) {
            if (errno != EINTR) {
                perror("epoll_wait");
                break;
            }
            continue; // Interrupted system call
        }
        
        for (int i = 0; i < n; i++) {
            if (events[i].data.fd == server_fd) {
                // Handle UDP datagram
                socklen_t client_len = sizeof(client_addr);
                
                // Receive data
                int bytes = recvfrom(server_fd, &recv_pkt, sizeof(packet_t), 0,
                              (struct sockaddr*)&client_addr, &client_len);
                
                if (bytes <= 0) {
                    if (bytes < 0 && errno != EAGAIN && errno != EWOULDBLOCK)
                        perror("recvfrom");
                    continue;
                }
                
                total_packets_received++;
                
                // Find or create client record
                int client_idx = find_client(clients, client_count, &client_addr);
                
                if (client_idx == -1) {
                    // New client
                    client_idx = add_client(clients, &client_count, &client_addr);
                    if (client_idx == -1) {
                        printf("Too many clients, dropping packet\n");
                        continue;
                    }
                    
                    char ip_str[INET_ADDRSTRLEN];
                    inet_ntop(AF_INET, &client_addr.sin_addr, ip_str, sizeof(ip_str));
                    printf("New client #%d from %s:%d\n", client_idx, ip_str, 
                           ntohs(client_addr.sin_port));
                }
                
                // Update last seen timestamp
                clients[client_idx].last_seen = time(NULL);
                
                // Process based on frame type
                if (recv_pkt.header.kind == DATA) {
                    char ip_str[INET_ADDRSTRLEN];
                    inet_ntop(AF_INET, &client_addr.sin_addr, ip_str, sizeof(ip_str));
                    
                    // Debug output
                    if (total_packets_received % 1000 == 0) {
                        printf("Received DATA from client %d (%s:%d): seq=%u, client_id=%u\n", 
                               client_idx, ip_str, ntohs(client_addr.sin_port),
                               recv_pkt.header.seq, recv_pkt.header.client_id);
                    }
                    
                    // Check if this is a new sequence number
                    if (recv_pkt.header.seq == clients[client_idx].last_seq) {
                        // Already processed this sequence, just resend ACK
                        init_packet(&send_pkt, ACK, 0, recv_pkt.header.client_id, 
                                    recv_pkt.header.seq, NULL);
                        
                        sendto(server_fd, &send_pkt, sizeof(packet_t), 0,
                               (struct sockaddr*)&client_addr, client_len);
                        
                        if (total_packets_received % 1000 == 0) {
                            printf("Resent ACK for seq=%u to client %d\n", 
                                   recv_pkt.header.seq, client_idx);
                        }
                    } else {
                        // New sequence number, update and send ACK
                        clients[client_idx].last_seq = recv_pkt.header.seq;
                        
                        // Process packet and send ACK
                        init_packet(&send_pkt, ACK, 0, recv_pkt.header.client_id, 
                                    recv_pkt.header.seq, NULL);
                        
                        // Copy the DATA packet payload for echo
                        memcpy(send_pkt.data, recv_pkt.data, PAYLOAD_SIZE);
                        
                        sendto(server_fd, &send_pkt, sizeof(packet_t), 0,
                               (struct sockaddr*)&client_addr, client_len);
                        
                        if (total_packets_received % 1000 == 0) {
                            printf("Sent ACK for seq=%u to client %d\n", 
                                   recv_pkt.header.seq, client_idx);
                        }
                    }
                } else {
                    // Unexpected packet type
                    char ip_str[INET_ADDRSTRLEN];
                    inet_ntop(AF_INET, &client_addr.sin_addr, ip_str, sizeof(ip_str));
                    printf("Received unexpected packet type %d from client %d (%s:%d)\n", 
                           recv_pkt.header.kind, client_idx, ip_str, 
                           ntohs(client_addr.sin_port));
                }
            }
        }
        
        // Check for timed-out clients (cleanup)
        time_t now = time(NULL);
        for (int j = 0; j < client_count; j++) {
            if (clients[j].active && now - clients[j].last_seen > 60) { // 60 seconds timeout
                char ip_str[INET_ADDRSTRLEN];
                inet_ntop(AF_INET, &clients[j].addr.sin_addr, ip_str, sizeof(ip_str));
                printf("Client %d (%s:%d) timed out\n", j, ip_str, 
                       ntohs(clients[j].addr.sin_port));
                
                // Mark as inactive
                clients[j].active = false;
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
