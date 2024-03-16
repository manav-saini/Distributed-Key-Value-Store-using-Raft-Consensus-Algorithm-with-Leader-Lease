import zmq
import json
import sys

class RaftClient:
    def __init__(self, nodes, client_id):
        self.nodes = nodes
        self.client_id = client_id
        self.context = zmq.Context()
        self.receiver = self.context.socket(zmq.PULL)
        self.receiver.bind(f"tcp://*:{self.client_id+10}")
        self.leader_id = None
        self.count = 0
    
    def is_connection_valid(self, node_id):
        try:
            context = zmq.Context()
            receiver = context.socket(zmq.REQ)
            receiver.bind(f"tcp://*:{node_id+10}")
            return False
        except Exception as e:
            return True

    def send_request(self, request):
        if self.leader_id is None:
            node_id = self.nodes[self.count]  # Default to the first node if none specified
        else:
            node_id = self.leader_id

        if not self.is_connection_valid(node_id) and self.count<len(self.nodes):
            print(f"Connection to node {node_id} is not valid. Unable to send request.")
            self.count += 1
            if self.count<len(self.nodes):
                return self.send_request(request)
            else:
                print("No valid connection to any node.")
                self.count = 0
                return None

        try:
            socket = self.context.socket(zmq.PUSH)
            socket.connect(f"tcp://localhost:{node_id+10}")
            print("CONNECT")
            socket.send_json(request)
            reply = self.receiver.recv_json()
            socket.close()
            print(reply)

            if reply['status'] == 'redirect' and 'leader_id' in reply:
                print(f"Redirecting to leader {reply['leader_id']}")
                self.leader_id = int(reply['leader_id'])
                return self.send_request(request)
            else:
                return reply
        except Exception as e:
            print(f"Error sending request to node {node_id}: {e}")
            return None

    def set_value(self, key, value):
        request = {'client_id': self.client_id, 'type': 'set', 'key': key, 'value': value}
        reply = self.send_request(request)
        return reply

    def get_value(self, key):
        request = {'client_id': self.client_id, 'type': 'get', 'key': key}
        reply = self.send_request(request)
        return reply

if __name__ == "__main__":
    nodes = list(range(5))  # Assuming 5 nodes with IDs 0-4
    client_id = int(input("Enter client ID: "))
    client = RaftClient(nodes, client_id)

    while True:
        command = input("Enter command (set key value | get key): ")
        command = command.split()
        if command[0] == "set" and len(command) == 3:
            key, value = command[1], command[2]
            print("Setting", key, "to", value)
            result = client.set_value(key, value)
            print("Set Result:", result)
        elif command[0] == "get" and len(command) == 2:
            key = command[1]
            result = client.get_value(key)
            print("Get Result:", result)
        else:
            print("Invalid command")