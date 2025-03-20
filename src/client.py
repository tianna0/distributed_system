import requests

SERVER_URL = "http://localhost:6000"

def create_topic(topic_name):
    url = f"{SERVER_URL}/topic"
    response = requests.put(url, json={"topic": topic_name})
    print("Create Topic:", response.status_code, response.json())

def list_topics():
    url = f"{SERVER_URL}/topic"
    response = requests.get(url)
    print("List Topics:", response.status_code, response.json())

def put_message(topic_name, message_content):
    url = f"{SERVER_URL}/message"
    response = requests.put(url, json={"topic": topic_name, "message": message_content})
    print("Put Message:", response.status_code, response.json())

def get_message(topic_name):
    url = f"{SERVER_URL}/message/{topic_name}"
    response = requests.get(url)
    print("Get Message:", response.status_code, response.json())

if __name__ == "__main__":
    topics = ["topic_A", "topic_B", "topic_C"]
    for topic in topics:
        create_topic(topic)
    
    list_topics()

    put_message("topic_A", "topic A message 1")
    put_message("topic_B", "topic B message 1")
    put_message("topic_C", "topic C message 1")

    get_message("topic_A")
    get_message("topic_B")
    get_message("topic_C")

    # get message that do not exist
    get_message("topic_A")
    # create duplicate topic
    create_topic("topic_A")

    get_message("unknown_topic")
