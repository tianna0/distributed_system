import time
import pytest
from test_utils import Swarm, LEADER, FOLLOWER

ELECTION_TIMEOUT = 2.0
NUM_NODES = 5  
PROGRAM_FILE_PATH = "src/node.py"

@pytest.fixture
def swarm():
    swarm = Swarm(PROGRAM_FILE_PATH, NUM_NODES)
    swarm.start(ELECTION_TIMEOUT)
    yield swarm
    swarm.clean()

def test_system_tolerates_half_nodes_down_and_recovers(swarm):
    """Test that the system functions with N/2-1 nodes down and recovers after restart."""

    leader = swarm.get_leader_loop(5)
    assert leader is not None, "No leader was elected."
    print(f"Initial Leader: Node {leader.i}")

    num_nodes_to_kill = (NUM_NODES // 2) - 1 
    killed_nodes = swarm.nodes[:num_nodes_to_kill] 
    for node in killed_nodes:
        node.clean()
        print(f"Killed Node {node.i}")

    topic = "test_topic"
    message = "hello world"

    leader.create_topic(topic)
    leader.put_message(topic, message)

    response = leader.get_message(topic)
    assert response.status_code == 200, "Leader failed to serve requests after node failure."
    assert response.json().get("message") == message, "Message queue did not function correctly."

    print("Cluster functioned correctly with N/2-1 nodes down.")

    for node in killed_nodes:
        node.start()
        print(f"Restarted Node {node.i}")

    time.sleep(10)  

    statuses = swarm.get_status()
    assert len(statuses) == NUM_NODES, f"Expected {NUM_NODES} active nodes, but found {len(statuses)}."
    print("All nodes successfully rejoined the cluster.")

    new_leader = swarm.get_leader_loop(15)
    assert new_leader is not None, "No leader was elected after recovery."
    print(f"Recovered Leader: Node {new_leader.i}")

    print("Test passed: System tolerated failure and recovered successfully.")
