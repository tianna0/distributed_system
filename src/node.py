from flask import Flask, request, jsonify
import threading
import random
import time
import requests
import json
import logging
import sys

app = Flask(__name__)

# Raft State
FOLLOWER = "Follower"
LEADER = "Leader"
CANDIDATE = "Candidate"

message_queue = {}
raft_state = {
    "role": FOLLOWER,
    "current_term": 0,
    "voted_for": None,
    "leader_id": None
}

# Build Log Class
from dataclasses import dataclass

@dataclass
class Log:
    term: int
    topic: str
    msg: str
    operation: int  # GET: -1 / PUT: 1

    def to_dict(self):
        return {
            "term": self.term,
            "topic": self.topic,
            "message": self.msg,
            "operation": self.operation,
        }

    @classmethod
    def to_instance(cls, data):
        return cls(
            term=data["term"],
            topic=data["topic"],
            msg=data["message"],
            operation=data["operation"],
        )

# Store log entries
log_entries = []
commit_index = -1


# Ensure correct usage
if len(sys.argv) != 3:
    print("Usage: python3 src/node.py config.json <node_id>")
    sys.exit(1)

CONFIG_PATH = sys.argv[1]
NODE_ID = int(sys.argv[2])  # Get node index from command-line

# Load configuration
with open(CONFIG_PATH, "r") as f:
    config = json.load(f)
NODES = config["addresses"]

# Ensure NODE_ID is valid
if NODE_ID < 0 or NODE_ID >= len(NODES):
    print(f"Error: NODE_ID {NODE_ID} is out of range. Must be between 0 and {len(NODES)-1}")
    sys.exit(1)

# Set correct port
PORT = NODES[NODE_ID]["port"]

# Timing Constants
ELECTION_TIMEOUT_RANGE = (0.15, 0.3) 
HEARTBEAT_INTERVAL = 0.1

# Locks
state_lock = threading.Lock()
election_timer = None
heartbeat_thread = None


# ------------------------- API Routes -------------------------

@app.route('/topic', methods=['PUT'])
def create_topic():
    """Allows only the leader to create a topic"""
    if raft_state["role"] != LEADER:
        return jsonify({"success": False, "error": "Not the leader"}), 403

    data = request.get_json()
    topic = data.get('topic')

    if not topic:
        return jsonify({'success': False }), 400
    if topic in message_queue:
        return jsonify({'success': False }), 400
    
    # message_queue[topic] = []
    # Write to log instead directly to message queue
    entry = Log(term=raft_state["current_term"], topic=topic, msg="create_topic", operation=1)
    log_entries.append(entry)
    replicate_log(entry) 

    return jsonify({'success': True})


@app.route('/topic', methods=['GET'])
def get_topics():
    return jsonify({'success': True, 'topics': list(message_queue.keys())})


@app.route('/message', methods=['PUT'])
def put_message():
    """Allows only the leader to add a message"""
    if raft_state["role"] != LEADER:
        return jsonify({"success": False, "error": "Not the leader"}), 403

    data = request.get_json()
    topic = data.get('topic')
    message = data.get('message')

    if not topic or not message:
        return jsonify({'success': False, 'error': 'Both topic and message are required'}), 400
    if topic not in message_queue:
        return jsonify({'success': False, 'error': 'Topic does not exist'}), 404

    # message_queue[topic].append(message)
    # Create log entry instead of directly writing
    entry = Log(term=raft_state["current_term"], topic=topic, msg=message, operation=1) # operation -- 1:PUT
    log_entries.append(entry)
    replicate_log(entry)

    return jsonify({'success': True})


@app.route('/message/<topic>', methods=['GET'])
def get_message(topic):
    """Allows only the leader to return messages"""
    if raft_state["role"] != LEADER:
        return jsonify({"success": False, "error": "Not the leader"}), 403

    if topic not in message_queue:
        return jsonify({'success': False}), 404
    if not message_queue[topic]:
        return jsonify({'success': False}), 404

    return jsonify({'success': True, 'message': message_queue[topic].pop(0)})


@app.route('/status', methods=['GET'])
def get_status():
    """Returns the role and current term of the node"""
    return jsonify({
        "role": raft_state["role"],
        "term": raft_state["current_term"]
    })


# ------------------------- Raft Leader Election -------------------------

@app.route('/request_vote', methods=['POST'])
def request_vote():
    data = request.get_json()
    candidate_term = data["term"]
    candidate_id = data["candidateId"]

    with state_lock:
        if candidate_term > raft_state["current_term"]:
            raft_state["current_term"] = candidate_term
            raft_state["voted_for"] = candidate_id
            raft_state["role"] = FOLLOWER
            return jsonify({"term": raft_state["current_term"], "voteGranted": True})

        elif raft_state["voted_for"] is None or raft_state["voted_for"] == candidate_id:
            raft_state["voted_for"] = candidate_id
            return jsonify({"term": raft_state["current_term"], "voteGranted": True})

    return jsonify({"term": raft_state["current_term"], "voteGranted": False})

def restart_election_timer():
    """Clears and restarts the election timer."""
    global election_timer
    if election_timer:
        election_timer.cancel()  # Cancel the existing timer

    election_timer = threading.Timer(random.uniform(*ELECTION_TIMEOUT_RANGE), start_election)
    election_timer.start()

@app.route('/append_entries', methods=['POST'])
def append_entries():
    data = request.get_json()
    leader_term = data["term"]
    leader_id = data["leaderId"]
    # Get entries from request
    entries = data.get("entries", [])

    with state_lock:
        if leader_term >= raft_state["current_term"]:
            raft_state["current_term"] = leader_term
            raft_state["leader_id"] = leader_id
            raft_state["role"] = FOLLOWER
            raft_state["voted_for"] = None  # Reset votes
            restart_election_timer()  # Reset election timeout

            # Store and apply logs from leader
            for entry_dict in entries:
                entry_obj = Log.to_instance(entry_dict)
                log_entries.append(entry_obj)
                apply_committed_entry(entry_obj)

            return jsonify({"term": raft_state["current_term"], "success": True})

    return jsonify({"term": raft_state["current_term"], "success": False})


# ------------------------- Background Processes -------------------------

def start_election():
    """Triggers an election if the leader is unresponsive"""
    global election_timer

    while True:
        timeout = random.uniform(*ELECTION_TIMEOUT_RANGE)
        time.sleep(timeout)

        with state_lock:
            if raft_state["role"] == LEADER:
                continue  # No election needed if already leader

            logging.info(f"Node {NODE_ID} starting an election (Term {raft_state['current_term'] + 1})")
            raft_state["role"] = CANDIDATE
            raft_state["current_term"] += 1
            raft_state["voted_for"] = NODE_ID
            votes = 1

        for node in NODES:
            if node["port"] == PORT:
                continue  # Skip self

            try:
                response = requests.post(
                    f"http://{node['ip']}:{node['port']}/request_vote",
                    json={"term": raft_state["current_term"], "candidateId": NODE_ID}
                ).json()

                if response.get("voteGranted"):
                    votes += 1

            except requests.RequestException:
                continue

        with state_lock:
            if votes > len(NODES) // 2:
                logging.info(f"Node {NODE_ID} won election for Term {raft_state['current_term']}")
                raft_state["role"] = LEADER
                heartbeat_thread = threading.Thread(target=send_heartbeats, daemon=True)
                heartbeat_thread.start()


def send_heartbeats():
    """Leader sends periodic heartbeats to prevent re-elections"""
    while raft_state["role"] == LEADER:
        for node in NODES:
            if node["port"] == PORT:
                continue  # Skip self

            # requests.post(
            #     f"http://{node['ip']}:{node['port']}/append_entries",
            #     json={"term": raft_state["current_term"], "leaderId": NODE_ID}
            # )

            # send empty entries as heartbeats
            try:
                requests.post(
                    f"http://{node['ip']}:{node['port']}/append_entries",
                    json={"term": raft_state["current_term"], "leaderId": NODE_ID, "entries": []},
                    timeout=0.5
                )
            except requests.RequestException:
                pass

        time.sleep(HEARTBEAT_INTERVAL)


# ------------------------- Log Replication -------------------------

def replicate_log(entry):
    """
    Leader calls this function to send new log entries to all followers
    and count the number of acks, and if more than half, apply to the local state machine.
    """
    ack_count = 1  
    total_nodes = len(NODES)

    for node in NODES:
        if node["port"] == PORT:
            continue
        try:
            response = requests.post(
                f"http://{node['ip']}:{node['port']}/append_entries",
                json={
                    "term": raft_state["current_term"],
                    "leaderId": NODE_ID,
                    "entries": [entry.to_dict()]
                },
                timeout=0.5
            )
            if response.status_code == 200 and response.json().get("success"):
                ack_count += 1
        except requests.RequestException:
            pass

    if ack_count > total_nodes // 2:
        apply_committed_entry(entry)


def apply_committed_entry(entry: Log):
    """
    Once the log reaches consensus on most nodes, it is written to the local state machine message_queue 
    """
    global commit_index
    commit_index += 1
    if entry.msg == "create_topic":
        if entry.topic not in message_queue:
            message_queue[entry.topic] = []
    else: 
        message_queue.setdefault(entry.topic, []).append(entry.msg)



if __name__ == '__main__':
    logging.info(f"Starting node on port {PORT}")
    election_thread = threading.Thread(target=start_election, daemon=True)
    election_thread.start()
    app.run(host='0.0.0.0', port=PORT, debug=True)