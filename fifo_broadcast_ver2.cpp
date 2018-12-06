#include "fifo_broadcast_ver2.h"

int parse_arguments(int argc, char** argv){
	/* 
	Initialze the fifo waiting and fifo buffer
	Initialize server and client socket
	Store peer addresses
	*/

	if (argc < 4){
		cerr << "No enough arguments" << endl;
		exit(-1);
	}

	PID = atoi(argv[1]) + 1;
	num_msg = atoi(argv[3]);

	char* host_file = argv[2];

	// Decode host file information
	ifstream host(host_file);

	string line;
	int count = 0;
	int sec_count = 0;
	if (host.is_open()){
		bool first_line = true;
		while(getline(host, line)){
			if (first_line){
				num_peers = stoi(line);
				first_line = false;
				continue;
			}
			if (count < num_peers){			
				// Get addresses info
				int first_delimiter = line.find(" ");
				string filtered_line = line.substr(first_delimiter + 1);

				int delimiter = filtered_line.find(" ");

				const char* ip_address = filtered_line.substr(0, delimiter).c_str();
				int port = atoi(filtered_line.substr(delimiter + 1).c_str());


				// set peer address
				struct sockaddr_in peer_addr;
				memset((wchar_t*) &peer_addr, 0, sizeof(peer_addr));
				peer_addr.sin_family = AF_INET;
				peer_addr.sin_port = htons(port);
				peer_addr.sin_addr.s_addr = inet_addr(ip_address);
				addresses[count] = peer_addr;

				// build server and client socket
				if (PID == count + 1){
					set_server_sock(ip_address, port);
				
					set_client_sock(ip_address, port);
				}
				count++;
				continue;
			}
			if (sec_count + 1 == PID){
				// Get causal target
				
				for (string::iterator it = line.begin();
					it != line.end();
					it++){
					if(*it == ' ')
						continue;
					causal_targets.insert((int) *it);
				}
				
			}
			sec_count++;
		}
	}

	cout << "Parameter decoding finished" << endl;
	return 0;
}

int set_server_sock(const char* ip_address, long port){
	
	if ((server_sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0){
		cerr << "Cannot create socket" << endl;
		exit(1);
	}

	struct sockaddr_in myaddr;

	memset((char*) &myaddr, 0, sizeof(myaddr));

	myaddr.sin_family = AF_INET;
	myaddr.sin_port = htons(port);
	myaddr.sin_addr.s_addr = inet_addr(ip_address);

	if (bind(server_sock, (struct sockaddr*) &myaddr, sizeof(myaddr)) < 0){
		cerr << "Cannot bind to server addr" << endl;
		exit(1);
	}

	cout << "Successfully set up server in " << ip_address << ':' \
	<< port << endl;

	return 0;
	
}


int set_client_sock(const char* ip_address, int port){
	if ((client_sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0){
		cerr << "Cannot create client socket" << endl;
		exit(1);
	}

	struct sockaddr_in myaddr;

	memset((wchar_t*) &myaddr, 0, sizeof(myaddr));

	myaddr.sin_family = AF_INET;
	myaddr.sin_port = htons(port + 20);
	myaddr.sin_addr.s_addr = inet_addr(ip_address);

	if (bind(client_sock, (struct sockaddr*) &myaddr, sizeof(myaddr)) < 0){
		cerr << "Cannot bind to client addr" << endl;
		exit(1);
	}

	cout << "Successfully set up client in " << ip_address << ':' \
	<< port + 20 << endl;

	return 0;
}

const Pkt& print_pkt(const Pkt& pkt){
	for (vector<int>::const_iterator it = pkt.begin();
		it != pkt.end();
		it++){
		cout << *it;
	}
	cout << endl;
	return pkt;
}

const Pkt build_pkt(const int msg_index){
	// [sender, history, pid, msg_index]
	Pkt pkt;
	// Add sender info to pkt
	pkt.insert(pkt.begin(), PID);
	// Add history to pkt
	for (vector<vector<int>>::iterator it = history.begin();
		it != history.end();
		it++){
		pkt.insert(pkt.end(), (*it).begin(), (*it).end());
	}
	// Add to send msg to pkt
	pkt.push_back(PID);
	pkt.push_back(msg_index);
	return pkt;
}

void encode_pkt(const Pkt& pkt){
	//cout << "encoding";
	//print_pkt(pkt);
	wchar_t* pkt_ptr = new wchar_t[4096];
	memset((wchar_t*) pkt_ptr, L'\0', 4096 * sizeof(wchar_t));
	for (int i = 0; i < pkt.size(); i++){
		pkt_ptr[i] = pkt[i];
	}
	// Put the wchar_t* to broadcast_pkts
	mtx.lock();
	broadcast_pkts.push_back(pkt_ptr);
	mtx.unlock();
}

void send_pkt(int peer){
	// Send all the msg to this peer
	// Get peer address
	struct sockaddr_in peer_addr = addresses[peer];
	int bytes;	
	while (true){
		mtx.lock();
		for (int i = 0; i < broadcast_pkts.size(); i++){
			bytes = sendto(client_sock,\
						 	broadcast_pkts[i],
							min(BUFFER_SIZE * sizeof(wchar_t) / sizeof(char),
								wcslen(broadcast_pkts[i]) * 4),
						 	0,
						 	(struct sockaddr*) &peer_addr,
						 	sizeof(peer_addr));
			if (bytes < 0)
				cerr << "Fail to send msg to " << peer << endl;
		}
		mtx.unlock();
		this_thread::sleep_for(chrono::milliseconds(10));
	}
	return;
}


void beb_broadcast(int peer){
	return;
}

void listen_pkt(){
	wchar_t pkt_str[BUFFER_SIZE];
	memset((wchar_t*) pkt_str, L'\0', BUFFER_SIZE * sizeof(wchar_t));
	struct sockaddr_in addr;
	socklen_t sock_size = sizeof(addr);
	int bytes = recvfrom(server_sock,\
						 pkt_str,\
						 BUFFER_SIZE * sizeof(wchar_t),\
						 0,\
						 (struct sockaddr*) &addr,
						 &sock_size);

	if (bytes <= 0){
		return;
	}
	if (bytes > 0){
		
		int pkt_size = wcslen(pkt_str);
		// Get sender
		int sender = pkt_str[0];
		// Get fpkt (including history)
		Pkt fpkt;
		for (int i = 1; i < pkt_size; i++){
			fpkt.push_back(pkt_str[i]);
		}
		// Get this pkt
		Pkt pkt;
		pkt.push_back(pkt_str[pkt_size - 2]);
		pkt.push_back(pkt_str[pkt_size - 1]);
		check_beb_deliver(sender, fpkt, pkt);
		
	}
	return;
}

void check_beb_deliver(int sender,\
						const Pkt& fpkt,\
						const Pkt& pkt){
	// Beb deliver if (sender, pkt.root, pkt.index) not in
	// beb_delivered
	Pkt local_pkt(pkt);
	local_pkt.insert(local_pkt.begin(), sender);
	if (beb_delivered.find(local_pkt) != beb_delivered.end()){
		return;
	}

	// Put new msg into beb_delivered
	// Question: the obj are stored by value or reference?
	beb_delivered.insert(local_pkt);
	beb_deliver(sender, fpkt, pkt);
	return;
}

void beb_deliver(int sender,
					const Pkt& fpkt,
					const Pkt& pkt){

	check_urb_deliver(sender, fpkt, pkt);
	return;
}

void check_urb_deliver(int sender,\
						const Pkt& fpkt,\
						const Pkt& pkt){
	// If already urb delivered, return
	if (urb_delivered.find(pkt) != urb_delivered.end())
		return;

	// If not forwarded, forward the pkt
	if (forwarded.find(pkt) == forwarded.end()){
		// Build forward pkt
		Pkt local_pkt(fpkt);
		local_pkt.insert(local_pkt.begin(), PID);
		encode_pkt(local_pkt);
		// Record forward info
		forwarded[pkt] = 0;
	}

	// Record ack info
	forwarded[pkt]++;

	// Check availability for urb delivery
	if (forwarded[pkt] > num_peers / 2){
		urb_deliver(sender, fpkt, pkt);
	}
}

void urb_deliver(int sender,\
					const Pkt& fpkt,
					const Pkt& pkt){

	check_causal_deliver(sender, fpkt, pkt);
	return;
}

void check_causal_deliver(int sender,\
							const Pkt& fpkt,
							const Pkt& pkt){
	// If already delivered, return
	if (causal_delivered.find(pkt) != causal_delivered.end()){
		return;
	}

	// Deliver the msg in full pkt in order
	// If a msg from causal target need to be delivered
	// Keep the history of the msg from causal target in 
	// local history
	vector<int>::const_iterator deliver_it = fpkt.begin();
	vector<int>::const_iterator history_it = fpkt.begin();
	Pkt local_pkt = {0, 0};
	Pkt history_pkt = {0, 0};
	while(deliver_it != fpkt.end()){
		// Check for causal delivery
		local_pkt[0] = *deliver_it;
		local_pkt[1] = *(deliver_it + 1);

		if (causal_delivered.find(local_pkt)\
			 == causal_delivered.end()){
			// Causal deliver msg
			causal_deliver(local_pkt);

			// If root is causal_target
			// Add history of this msg to local history
			if (causal_targets.find(local_pkt[0])\
			 	!= causal_targets.end()){
				while (history_it != deliver_it){
					history_pkt[0] = *(history_it);
					history_it++;
					history_pkt[1] = *(history_it);
					history_it++;
					if (find(history.begin(), history.end(), history_pkt) == history.end())
						history.push_back(history_pkt);
				}
				history_pkt[0] = *(history_it);
				history_it++;
				history_pkt[1] = *(history_it);
				history_it++;
				if (find(history.begin(), history.end(), history_pkt)== history.end())
					history.push_back(history_pkt);
			}

		}
		this_thread::sleep_for(chrono::milliseconds(10));
	advance(deliver_it, 2);
	}
	return;
}

void causal_deliver(const Pkt& pkt){
	cout << "Causal deliver";
	print_pkt(pkt);
	causal_delivered.insert(pkt);
	return;
}


void start_listen(){
	while(true){
		listen_pkt();
	}
}

void start(int signum){
	START = true;
	cout << "Start broadcasting" << endl;
	return;
}

void stop(int signum){
	cout << "End" << endl;
	fclose(stdout);
	close(server_sock);
	close(client_sock);
	exit(0);
	
	return;
}
int main(int argc, char** argv){

	// Register signal handler
	signal(SIGUSR1, start);
	signal(SIGTERM, stop);
	signal(SIGINT, stop);

	// Parse arguments
	parse_arguments(argc, argv);
	//string tmp = "da_proc_" + PID;
	//string file_name = tmp + ".lov";
	//freopen(file_name.c_str(), "w", stdout);
	/*
	while(!START){
		this_thread::sleep_for(chrono::milliseconds(100));
	}
	*/
	
	// Listen
	threads.push_back(thread(start_listen));

	// Start sending in case of need for forwarding
	for (int peer = 0; peer < num_peers; peer++){
		threads.push_back(thread(send_pkt, peer));
	}
	this_thread::sleep_for(chrono::milliseconds(500));
	// Broadcast msgs
	for (int i = 0; i < num_msg; i++){
		this_thread::sleep_for(chrono::milliseconds(100));

		Pkt pkt = build_pkt(i + 1);
		encode_pkt(pkt);
		// threads.push_back(thread(beb_broadcast, pkt, false));
		history.push_back({pkt[pkt.size() - 2],
							pkt[pkt.size() - 1]});
		cout << "sending " << i + 1 << "th msg" << endl;
 	}


	while(true);

	// End
	
	for (vector<thread>::iterator it = threads.begin();
		it != threads.end();
		it++){
		(*it).join();
	}
	
	return 0;
}
