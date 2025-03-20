import threading
import pytest
import time
from test_utils import Swarm

TEST_TOPIC = "concurrent_test_topic"
TEST_MESSAGES = [f"Message {i}" for i in range(10)]
PROGRAM_FILE_PATH = "src/node.py"
ELECTION_TIMEOUT = 2.0


@pytest.fixture
def node_with_test_topic():
    node = Swarm(PROGRAM_FILE_PATH, 1)[0]
    node.start()
    time.sleep(ELECTION_TIMEOUT)
    assert node.create_topic(TEST_TOPIC).json() == {"success": True}
    yield node
    node.clean()


def put_message_concurrently(node, topic, message, results):
    response = node.put_message(topic, message).json()
    results.append(response)


def get_message_concurrently(node, topic, results):
    response = node.get_message(topic).json()
    results.append(response)


def test_concurrent_put_and_get_operations(node_with_test_topic):
    """Tests concurrent PUT and GET operations to check for race conditions."""
    put_threads = []
    get_threads = []
    put_results = []
    get_results = []

    for message in TEST_MESSAGES:
        thread = threading.Thread(target=put_message_concurrently, args=(node_with_test_topic, TEST_TOPIC, message, put_results))
        put_threads.append(thread)
        thread.start()

    for thread in put_threads:
        thread.join()

    for _ in range(len(TEST_MESSAGES)):
        thread = threading.Thread(target=get_message_concurrently, args=(node_with_test_topic, TEST_TOPIC, get_results))
        get_threads.append(thread)
        thread.start()

    for thread in get_threads:
        thread.join()

    received_messages = [res["message"] for res in get_results if res["success"]]
    assert sorted(received_messages) == sorted(TEST_MESSAGES), f"Mismatch messages: {received_messages}"
