#include<iostream>
#include<thread>
#include<vector>
#include<sys/socket.h>
#include<sys/types.h>
#include<mutex>
#include<stdlib.h>
#include<string>
#include<fstream>
#include<string.h>
#include<set>
#include<map>
#include<chrono>
#include<algorithm>
#include<csignal>
#include<stdio.h>
#include<unistd.h>
#include<sstream>
#include <arpa/inet.h>

using namespace std;

#define MAX_NEIGHBOURS 5
#define MUTEX_NUMBER 1024

typedef vector<int> Pkt;
typedef vector<vector<int>> History;

// Define struct of address
struct Address{
	char* ip_address;
	int port;
};

// Define necessary global variable

// Variables for signal handling
bool START = false;

// Listen buffer size 
int BUFFER_SIZE = 4096;

// Raw address vector
vector<Address> peer_addresses;

// Address vector
struct sockaddr_in addresses[MAX_NEIGHBOURS];

// Broadcast msg vector 
vector<wchar_t*> broadcast_pkts;

// Beb delivered vector
set<Pkt> beb_delivered;

// Urb forwarded map
map<Pkt, int> forwarded;

// Urb delivered set
set<Pkt> urb_delivered;

// Local causal target
set<int> causal_targets;

// Causal delivery history
History history;

// Causal delivered set
set<Pkt> causal_delivered;

// Lock
mutex mtx;
mutex log_lock;

// Threads
vector<thread> threads;

// Log file
FILE* log_file;

// Client and server socket
long client_sock;
long server_sock;

// Number of peers
int num_peers;

// Number of msg to send
int num_msg;

// Self pid
int PID;

// Functions declarations
void start(int signum);

int parse_arguments(int argc, char** argv);

int set_server_sock(const char* ip_address, long port);

int set_client_sock(const char* ip_address, int port);

int set_peer_addr(char* ip_address, int port);

const Pkt build_pkt(const int index);

void encode_pkt(const Pkt& pkt);

void beb_broadcast(int peer);

void send_pkt(int peer);

void urb_broadcast(const Pkt);

void fifo_broadcast(const Pkt);

void listen();

void start_listen();

void check_beb_deliver(int sender,\
						const Pkt& fpkt,\
						const Pkt& pkt);

void check_urb_deliver(int sender,\
						 const Pkt& fpkt,\
						 const Pkt& pkt);

void beb_deliver(int sender,\
					const Pkt& fpkt,\
					const Pkt& pkt);

void urb_deliver(int sender,\
					const Pkt& fpkt,\
					const Pkt& pkt);

void check_causal_deliver(int sender,
							const Pkt& fpkt,
							const Pkt& pkt);

void causal_deliver(const Pkt& pkt);