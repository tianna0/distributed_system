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

def test_replication_persists_after_leader_crash(swarm):
    """
    Ensures that after a leader crashes and a new one is elected,
    all committed messages are consistence.
    """

    leader = swarm.get_leader_loop(5)
    assert leader is not None, "No leader was elected."

    topic = "replication_test_topic"
    message = "replicated_message"

    assert leader.create_topic(topic).json()["success"], "Failed to create topic"
    assert leader.put_message(topic, message).json()["success"], "Failed to put message"

    time.sleep(3) 

    # Kill current leader and elect new
    leader.clean()
    new_leader = swarm.get_leader_loop(10)
    assert new_leader is not None, "No leader was elected."
    print(f"New Leader: Node {new_leader.i}")

    response = new_leader.get_message(topic)
    assert response.status_code == 200, "Failed to retrieve message."
    assert response.json().get("message") == message, "Replicated message was lost."

