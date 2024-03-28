import zmq
import threading
import time
import json
from enum import Enum
import random
import os
import signal

class NodeState(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

class RaftNode:
    def __init__(self, node_id, cluster_nodes):
        self.node_id = node_id
        self.state = NodeState.FOLLOWER
        self.term = 0
        self.vote_count = 0
        self.voted_for = None
        self.log = []
        self.commit_index = 0
        self.last_applied = 0
        self.cluster_nodes = cluster_nodes
        self.leader_id = None
        self.election_timeout = random.uniform(5, 10)
        self.heartbeat_interval = 1
        self.lease_duration = 10
        self.lease_expiry = time.time()
        self.context = zmq.Context()
        self.receiver = self.context.socket(zmq.PULL)
        self.receiver.bind(f"tcp://*:{self.node_id+10}")
        self.sender = self.context.socket(zmq.PUSH)
        self.election_timer = threading.Timer(self.election_timeout, self.start_election)
        self.election_timer.start()
        self.next_index = {node_id: len(self.log) for node_id in cluster_nodes}
        self.match_index = {node_id: 0 for node_id in cluster_nodes}
        self.message_listener_thread = threading.Thread(target=self.start)
        self.message_listener_thread.start()
        self.state_machine = {}
        self.lease_end_time = 0
        self.old_leader_lease_expiry = 0
        self.append_count =0
        self.log_file = f"logs_node_{self.node_id}/log.txt"
        self.metadata_file = f"logs_node_{self.node_id}/metadata.json"
        self.dump_file = f"logs_node_{self.node_id}/dump.txt"
        self.ensure_log_directory_exists()
        self.load_state_from_disk()
        signal.signal(signal.SIGINT, self.graceful_shutdown)
        signal.signal(signal.SIGTERM, self.graceful_shutdown)
        lease_timeout_observer_thread = threading.Thread(target=self.observe_lease_timeout)
        lease_timeout_observer_thread.start()
        self.heartbeat_replies = set()
        self.heartbeat_reply_lock = threading.Lock()
        self.heartbeat_timeout = self.heartbeat_interval * 2
    def dump_file_state(self, message):
        with open(self.dump_file, "a") as df:
            df.write(json.dumps(message) + "\n")
            print(f"Dumped message to {self.dump_file}: {message}")

    def graceful_shutdown(self, signum, frame):
        print(f"Node {self.node_id} is shutting down gracefully...")
        self.save_state_to_disk()
        self.receiver.close()
        self.sender.close()
        self.context.term()
        exit(0)
    
    def ensure_log_directory_exists(self):
        os.makedirs(f"logs_node_{self.node_id}", exist_ok=True)
    
    def save_state_to_disk(self):
        with open(self.log_file, "w") as lf:
            for entry in self.log:
                lf.write(json.dumps(entry) + "\n")
                print(f"Saved log entry to {self.log_file}: {entry}")
                
        metadata = {
            "term": self.term,
            "voted_for": self.voted_for
        }
        with open(self.metadata_file, "w") as mf:
            json.dump(metadata, mf)
            print(f"Saved metadata to {self.metadata_file}: {metadata}")
    
    def load_state_from_disk(self):
        if os.path.exists(self.log_file):
            with open(self.log_file, "r") as lf:
                self.log = [json.loads(line.strip()) for line in lf]
                print(f"Loaded log from {self.log_file}: {self.log}")
        
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, "r") as mf:
                metadata = json.load(mf)
                self.term = metadata.get("term", 0)
                self.voted_for = metadata.get("voted_for", None)
                print(f"Loaded metadata from {self.metadata_file}: {metadata}")
    
    def start_election(self):
        if self.state != NodeState.LEADER:
            self.state = NodeState.CANDIDATE
            self.term += 1
            self.vote_count = 1
            self.voted_for = self.node_id
            self.leader_id = None
            self.request_votes()
            self.reset_election_timer()
            print(f"Started election in term {self.term} as candidate {self.node_id}")
        
    def request_votes(self):
        message = {
            'type': 'vote_request',
            'term': self.term,
            'candidate_id': self.node_id,
            'last_log_index': len(self.log) - 1,
            'last_log_term': self.log[-1]['term'] if self.log else 0
        }
        for node_id in self.cluster_nodes:
            if node_id != self.node_id:
                self.send_message(node_id, message)
                print(f"Sent vote request to node {node_id} for term {self.term}")
    
    def send_message(self, node_id, message):
        self.sender.connect(f"tcp://localhost:{node_id+10}")
        self.sender.send_json(message)
        self.sender.disconnect(f"tcp://localhost:{node_id+10}")

    def receive_messages(self):
        while True:
            message = self.receiver.recv_json()
            if message['type'] == 'vote_request':
                self.handle_vote_request(message)
            elif message['type'] == 'vote_reply':
                self.handle_vote_reply(message)
            print(f"Received message: {message}")
                
    def handle_vote_request(self, message):
        grant_vote = False
        if message['term'] > self.term:
            self.term = message['term']
            self.state = NodeState.FOLLOWER
            self.voted_for = None
        if self.voted_for is None or self.voted_for == message['candidate_id']:
            if self.is_log_up_to_date(message['last_log_index'], message['last_log_term']):
                grant_vote = True
                self.voted_for = message['candidate_id']
                self.reset_election_timer()
        
        reply = {
            'type': 'vote_reply',
            'term': self.term,
            'vote_granted': grant_vote
        }
        self.send_message(message['candidate_id'], reply)
        print(f"Handled vote request: {message}, granted vote: {grant_vote}, sent reply: {reply}")

    def handle_vote_reply(self, message):
        if message['term'] == self.term and message['vote_granted']:
            self.vote_count += 1
            if self.vote_count > len(self.cluster_nodes) / 2:
                self.become_leader()

    def become_leader(self):
        if self.state == NodeState.CANDIDATE:
            self.state = NodeState.LEADER
            self.leader_id = self.node_id
            print(f"Node {self.node_id} became the leader for term {self.term}.")
            self.wait_for_old_leader_lease_expiry()
            self.log.append({'term': self.term, 'type': 'NO-OP'})
            self.next_index[self.node_id] = len(self.log)
            self.match_index[self.node_id] = len(self.log) - 1
            self.update_lease_end_time()
            self.schedule_heartbeat()
            self.save_state_to_disk()
            self.append_entries()
            heartbeat_reply_observer_thread = threading.Thread(target=self.observe_heartbeat_replies)
            heartbeat_reply_observer_thread.start()

    def wait_for_old_leader_lease_expiry(self):
        if self.old_leader_lease_expiry:
            while time.time() < self.old_leader_lease_expiry + self.lease_end_time:
                time.sleep(1)
            print("Old leader's lease has expired. Continuing as the leader.")
        else:
            print("No old leader lease expiry information. Continuing as the leader.")

    # def handle_append_replies(self, message):
    #     if message['success']:
    #         self.apend_count += 1
    #         self.append_nodes.append(message['node_id'])
    #         if self.apend_count > len(self.cluster_nodes) / 2:
    #             for node_id in self.append_nodes:
    #                 if node_id not in self.append_done_nodes:
    #                     self.match_index[node_id] = message['match_index']
    #                     self.next_index[node_id] = message['match_index'] + 1
    #                     self.append_done_nodes.append(node_id)
    #             self.commit_index = message['match_index']
    #             self.apply_to_state_machine(self.commit_index)
    #             self.append_entries()


    def handle_append_replies(self, message):
        if message['success']:
            self.append_count += 1
            if self.term==message['term'] and self.append_count > len(self.cluster_nodes) / 2:
                self.commit_index = message['match_index']
                for i in range( self.last_applied, self.commit_index + 1):
                    try:
                        self.state_machine[self.log[i]['key']] = self.log[i]['value']
                    except:
                        continue
                for node_id in self.cluster_nodes:
                    self.match_index[node_id] = message['match_index']
                    self.next_index[node_id] = message['match_index'] + 1
                    thread = threading.Thread(target=self.append_entries, args=("committed",))
                    thread.start()
                    thread.join()
                self.last_applied = self.commit_index
    
    def update_lease_end_time(self):
        self.lease_end_time = time.time() + self.lease_duration
    
    def schedule_heartbeat(self):
        if self.state == NodeState.LEADER:
            self.send_heartbeat_to_all_followers()
            threading.Timer(self.heartbeat_interval, self.schedule_heartbeat).start()
    
    def send_heartbeat_to_all_followers(self):
        for node_id in self.cluster_nodes:
            self.dump_file_state(f"Leader {self.node_id} sending heartbeat to {node_id}")
            if node_id != self.node_id:
                message = {
                    'type': 'heartbeat',
                    'term': self.term,
                    'leader_id': self.node_id,
                    'lease_end_time': self.lease_end_time
                }
                self.send_message(node_id, message)
                print(f"Sent heartbeat to node {node_id}")

    
    def handle_heartbeat(self, message):
        if message['term'] >= self.term:
            self.term = message['term']
            self.state = NodeState.FOLLOWER
            self.leader_id = message['leader_id']
            self.lease_end_time = message['lease_end_time']
            self.old_leader_lease_expiry = message['lease_end_time']
            self.reset_election_timer()
            print(f"Received heartbeat from leader {self.leader_id}")
            self.update_lease_end_time()
            mssg = {
                    'type': 'ack',
                    'term': self.term,
                    'node_id': self.node_id,
                    'lease_end_time': self.lease_end_time
                }
            self.send_message(message["leader_id"], mssg)

    def handle_ack(self, message):
        if message['term'] == self.term:
            self.heartbeat_reply_lock.acquire()
            self.heartbeat_replies.add(message['node_id'])
            self.heartbeat_reply_lock.release()

    def observe_heartbeat_replies(self):
        while True:
            if self.state == NodeState.LEADER:
                time.sleep(1)  # Check heartbeat replies every second
                self.heartbeat_reply_lock.acquire()
                if len(self.heartbeat_replies) > len(self.cluster_nodes) // 2:
                    # Majority of followers have replied to the heartbeat
                    self.update_lease_end_time()  # Reset the lease timer
                    print("______________________________")
                    print(f"Received majority heartbeat replies. Reset lease timer for leader {self.node_id}.")
                    print("________________________________________")
                self.heartbeat_replies.clear()  # Clear the heartbeat replies set
                self.heartbeat_reply_lock.release()
            else:
                break  # If not leader, exit the thread
            time.sleep(1) 

    def is_leader_lease_valid(self):
        return time.time() < self.lease_end_time
    
        
    def reset_election_timer(self):
        if self.election_timer.is_alive():
            self.election_timer.cancel()
        self.election_timer = threading.Timer(self.election_timeout, self.start_election)
        self.election_timer.start()
        print(f"Reset election timer for term {self.term}")

    def is_log_up_to_date(self, last_log_index, last_log_term):
        try:
            if len(self.log) > 0:
                lastTerm = self.log[-1].term
            logOk = (last_log_term > lastTerm) or (last_log_term == lastTerm and last_log_index >= len(self.log))
            return logOk
        except:
            return True

    def append_entries(self, state="uncommitted"):
        for node_id in self.cluster_nodes:
            if node_id != self.node_id:
                entries = self.log[self.next_index[node_id]:]
                message = {
                    'type': 'append_entries',
                    'term': self.term,
                    'state': state,
                    'leader_id': self.node_id,
                    'prev_log_index': self.next_index[node_id] - 1,
                    'prev_log_term': self.log[self.next_index[node_id] - 1]['term'] if self.next_index[node_id] > 0 else 0,
                    'entries': entries,
                    'leader_commit': self.commit_index,
                    'lease_duration': self.lease_duration
                }
                self.send_message(node_id, message)
        self.save_state_to_disk()
        print(f"Appended entries: {entries}")

    def handle_append_entries(self, message):
        if message["state"]=="uncommitted":
            response = {
                'type': 'append_reply',
                'term': self.term,
                'success': False,
                'match_index': -1,
                'index':-1
            }
            if message['term'] >= self.term:
                self.term = message['term']
                self.state = NodeState.FOLLOWER
                self.leader_id = message['leader_id']
                self.voted_for = None 
                self.reset_election_timer()
            if self.log_consistency_check(message['prev_log_index'], message['prev_log_term']):
                self.next_index[message['leader_id']] = message['prev_log_index'] + len(message['entries']) + 1
                self.match_index[message['leader_id']] = message['prev_log_index'] + len(message['entries'])
                index = message['prev_log_index'] + 1
                response['index'] = index
                for entry in message['entries']:
                    if index < len(self.log):
                        if self.log[index]['term'] != entry['term']:
                            self.log = self.log[:index]
                    else:
                        self.log.append(entry)
                    index += 1

                response['success'] = True
                response['match_index'] = len(self.log) - 1
                if message['leader_commit'] > self.commit_index:
                    self.commit_index = min(message['leader_commit'], len(self.log) - 1)
            self.send_message(message['leader_id'], response)
        else:
            self.apply_to_state_machine(self.commit_index)
            self.last_applied = self.commit_index
        self.save_state_to_disk()

    def log_consistency_check(self, prev_log_index, prev_log_term):
        if prev_log_index == -1:
            return True
        if prev_log_index < len(self.log) and self.log[prev_log_index]['term'] == prev_log_term:
            return True
        return False
    
    def client_request_handler(self, request):
        print("INNNNNNN")
        if self.state == NodeState.LEADER:
            if request['type'] == 'set'and self.is_leader_lease_valid():
                entry = {'term': self.term, 'key': request['key'], 'value': request['value']}
                self.log.append(entry)
                self.next_index[self.node_id] = len(self.log)
                self.match_index[self.node_id] = len(self.log) - 1
                self.append_entries()
                return {"status": "success", "message": "Value set successfully"}
            elif request['type'] == 'get':
                for entry in reversed(self.log):
                    if entry['key'] == request['key']:
                        return {"status": "success", "value": entry['value']}
                return {"status": "failure", "message": "Key not found"}
        else:
            if self.leader_id is not None:
                return {"status": "redirect", "leader_id": self.leader_id}
            else:
                return {"status": "failure", "message": "Leader unknown"}
    
    def apply_to_state_machine(self, upto_index):
        for i in range(self.last_applied, upto_index + 1):
            entry = self.log[i]
            try:
                self.state_machine[entry['key']] = entry['value']
                self.last_applied = i
            except:
                continue
        print(f"Node {self.node_id} applied entries up to index {upto_index} to state machine.")

    def observe_lease_timeout(self):
        while True:
            if self.state == NodeState.LEADER and not self.is_leader_lease_valid():
                print(f"Leader {self.node_id} lease timed out. Stepping down to follower.")
                self.state = NodeState.FOLLOWER
                self.leader_id = None
            time.sleep(1)
    
    def start(self):
        try:
            while True:
                message = self.receiver.recv_json()
                if message['type'] == 'set'or message['type'] == 'get':
                    response = self.client_request_handler(message)
                    self.send_message(message['client_id'], response)
                elif message['type'] == 'append_entries':
                    self.handle_append_entries(message)
                elif message['type'] == 'vote_request':
                    print("Vote Request")
                    self.handle_vote_request(message)
                elif message['type'] == 'vote_reply':
                    print("Vote Reply")
                    self.handle_vote_reply(message)
                elif message['type'] == 'heartbeat':
                    self.handle_heartbeat(message)
                elif message['type'] == 'ack':
                    self.handle_ack(message)
                elif message['type'] == 'append_reply':
                    self.handle_append_replies(message)
        except KeyboardInterrupt:
            self.graceful_shutdown()

cluster_nodes = [0, 1, 2, 3, 4]

node_num = int(input("Enter the node number: "))
node = RaftNode(node_num, cluster_nodes)
