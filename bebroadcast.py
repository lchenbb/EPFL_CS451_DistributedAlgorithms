
'''
AUTHOR: Liangwei Chen
NAME: Best effort broadcast
DATE CREATED: 28/10/2018
DATE LAST MODIFIED: 28/10/2018
PYTHON VER: 3.6.5
'''

import sys
import time
import socket
import logging
import threading
import collections

try:
	SCRIPT = sys.argv[0]
	HOST_FILE = sys.argv[1]
	PID = int(sys.argv[2])
except:
	print('Execetuting pattern should be: \
		python {0} host_file pid'.format(SCRIPT))
	sys.exit(-1)

# Timer interval
INTERVAL = 0.25

# Packet size
SIZE = 1024

# Set messages
MESSAGES = list(range(20))
MESSAGES_SIZE = len(MESSAGES)

# Obtain communication peers information
with open(HOST_FILE, 'r') as f:

	ADDRESSES = [tuple([line.rstrip('\n')\
							.split(',')[0],
						int(line.rstrip('\n').split(',')[1])]) for line in f if len(line) > 1]

	NUM_PEERS = len(ADDRESSES)

# Initialize UDP sockets
client_socks = [socket.socket(socket.AF_INET,
							socket.SOCK_DGRAM) for i in range(NUM_PEERS)]

server_sock = socket.socket(socket.AF_INET,
							socket.SOCK_DGRAM)
server_sock.bind(ADDRESSES[PID])

def set_logger(logger_name, log_file_addr):
	# set up logger to write broadcast info
	# params: logger_name: str
	#		  log_file: str
	# return: logger: logging.Logger

	# Get or create logger
	l = logging.getLogger(logger_name)
	l.setLevel(logging.DEBUG)

	# Bind logger to log file
	log_file = logging.FileHandler(log_file_addr, 'w')
	l.addHandler(log_file)

	return l

class Sender(threading.Thread):
	# Handler of sending msg


	def __init__(self, t_pid, payload):
		# Constructor
		# Params: t_pid: int
		# 	      payload: tuple
		# Return: None

		threading.Thread.__init__(self)
		self.t_pid = t_pid
		self.payload = str((PID, payload))

	def run(self):
		# start UDP sending objects
		# Params: 
		# Return:
		while True:
			client_socks[self.t_pid].sendto(bytes(self.payload, 'utf-8'),\
							ADDRESSES[self.t_pid])



class PacketPubSub:
	# Handler of packet publishment and subscription


	def __init__(self):
		# Constructor
		# Params: 
		# Return:

		self.subscribing = None

	def add_subscribing_func(self, notify):
		# Add subscribing function to packet listener
		# Params: notify: func(str)
		# Return: None

		self.subscribing = notify

	def notify(self, sender, payload):
		# Handle payload returned by packet listener
		# Params: sender: int
		#	      payload: str
		# Return: None

		self.subscribing(sender, payload)

	def start_packet_listen(self):
		# Create listener to listen to server socket
		# Params: 
		# Return:

		listener = self.Packet_listener(self.notify)
		listener.setDaemon(True)
		listener.start()

	class Packet_listener(threading.Thread):
		# Listener to packets from the socket

		def __init__(self, notify):
			# Constructor
			# Params: notify: func(str)
			threading.Thread.__init__(self)
			self.notify = notify

		def run(self):
			# execute listener thread
			# Params: 
			# Return: 

			while True:


				# Get packet
				pkt = server_sock.recv(1024)

				# Decode sender
				sender, payload = eval(pkt)

				# Notify upper layer handler
				self.notify(sender, payload)


							



class BEBroadcast:
	# Best Effort Broadcast class


	def __init__(self, notify):
		# Constructor: add subcribing function to PacketPubSub obj
		# Params: nofity: None ()(int sender, str payload)
		# Return:
		self.delivered = set()
		packet_pub_sub.add_subscribing_func(self.deliver)
		self.notify = notify

	def broadcast(self, payload):
		# Best Effort Broadcast payload to every peer
		# Params: payload: object
		# Return: None

		for pid in range(NUM_PEERS):
			sender = Sender(pid, payload)
			sender.setDaemon(True)
			sender.start()


	def deliver(self, sender, payload):
		# Deliver payload
		# Params: payload: str
		# Return: None

		if (sender, payload) not in self.delivered:
			self.delivered.add((sender, payload))
			self.notify(sender, payload)

class URBBroadcast:
	# Uniform reliable broadcast


	def __init__(self, notify = None):
		# Initialization
		# Params: 
		# Return:

		self.delivered = set()
		self.ack = collections.defaultdict(set)
		self.beb = BEBroadcast(self.check_delivery)
		self.notify = notify
		self.lock = threading.Lock()

	def broadcast(self, msg):
		# Uniform reliable broadcast message
		# Params: msg: str
		# Return: 

		payload = (PID, msg)
		self.beb.broadcast(payload)
		l.debug('Uniform reliable broadcast: {0}'.format(str(msg)))
		print('Uniform reliable broadcast: {0}'.format(msg))

	def check_delivery(self, sender, payload):
		# Check whether received ack from more than half
		# processes if msg not delivered
		# Params: sender: int
		#         payload: bytes
		# Return:

		# Get root of msg and msg content
		root, msg = payload

		# Check delivery of msg
		if (root, msg) in self.delivered:
			return

		# Forward if not forwarded
		if len(self.ack[payload]) == 0:
			self.beb.broadcast(payload)

		# Ack sender
		self.ack[payload].add(sender)

		# Check delivery
		if len(self.ack[(root, msg)]) > NUM_PEERS // 2:
			self.deliver(root, msg)

	def deliver(self, root, msg):
		# Uniform reliable deliver msg
		# Params: root: int
		# Return: 

		if self.notify is not None:
			# If need to notify higher level function to deal
			# message, notify it
			#self.lock.acquire()
			self.notify(root, msg)
			#self.lock.release()

		# print(threading.get_ident())
		l.debug('Uniform reliable deliver msg {0} from {1}'.format(msg, root))
		print('Uniform reliable deliver msg {0} from {1}'.format(msg, root))

class FIFOBroadcast:
	# Uniform reliable FIFO Broadcast


	def __init__(self):
		# Initialize urb, require dict, buffer
		# Params: 
		# Return:

		self.urb = URBBroadcast(self.check_delivery)
		self.require_dict = collections.defaultdict(int)
		self.buffer = collections.defaultdict(dict)

	def broadcast(self, seq_n, msg):
		# FIFO broadcast message with index
		# Params: seq_n: int
		#		  msg: object
		# Return: 

		indexed_msg = (seq_n, msg)

		self.urb.broadcast(indexed_msg)

		l.debug('FIFO broadcast: index {0}, value{1}'.format(seq_n, msg))
		print('FIFO broadcast: index {0}, value{1}'.format(seq_n, msg))
		
	def check_delivery(self, root, indexed_msg):
		# If msg is required, deliver it, else buffer it
		# Params: root: int
		# 		  indexed_msg: tuple

		seq_n, msg = indexed_msg

		require = self.require_dict[root]

		if seq_n == require:
			# Deliver if index of msg is wanted	
			
			# Deliver the msg
			self.deliver(root, seq_n, msg)

			# Increment require
			require += 1

			while True:
				# Deliver adjacent buffered msg

				if require in self.buffer[root].keys():

					# Deliver adjacent msg
					'''
						Possible to optimize by eliminating 
						delivered msg from buffer
					'''
					self.deliver(root, require, self.buffer[root][require])

					# Increment require
					require += 1

					continue

				break

			self.require_dict[root] = require
		else: 
			self.buffer[root][seq_n] = msg

	def deliver(self, root, seq_n, msg):
		# FIFO deliver msg
		# Params: root: int
		#		  seq_n: int
		#         msg: object
		# Return:

		l.debug('FIFO deliver from {0}, index {1}, value {2}'.\
			format(root, seq_n, str(msg)))
		print('FIFO deliver from {0}, index {1}, value {2}'.\
			format(root, seq_n, str(msg)))

if __name__ == '__main__':


	# Set up logger
	l = set_logger('main', 'process_{0}.log'.format(PID))

	# Create Packet Publishing Subsribing Object
	packet_pub_sub = PacketPubSub()

	# Create broadcast object
	fifo = FIFOBroadcast()

	# Start listening 
	packet_pub_sub.start_packet_listen()

	# Broadcast all messages
	for index, m in enumerate(MESSAGES):
		fifo.broadcast(index, m)

	while True:
		pass
