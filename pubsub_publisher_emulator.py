from datetime import datetime
from random import random
from google.auth import jwt
from google.cloud import pubsub_v1
import base64
import time
import json

# --- Base variables and auth path
CREDENTIALS_PATH = "credentials/[PROJECT_ID].json"
PROJECT_ID = "[PROJECT_ID]"
TOPIC_ID = "test"
MAX_MESSAGES = 500000


# --- PubSub Utils Classes
class PubSubPublisher:
    def __init__(self, credentials_path, project_id, topic_id):
        credentials = jwt.Credentials.from_service_account_info(
            json.load(open(credentials_path)),
            audience="https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
        )
        self.project_id = project_id
        self.topic_id = topic_id
        self.publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
        self.publisher = pubsub_v1.PublisherClient(publisher_options=self.publisher_options, credentials=credentials)
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

    def publish(self, data: str):
        result = self.publisher.publish(self.topic_path, data=data, ordering_key='test')
        return result


# --- Main publishing script
def main():
    i = 0
    publisher = PubSubPublisher(CREDENTIALS_PATH, PROJECT_ID, TOPIC_ID)
    while i < MAX_MESSAGES:
        body = {
            "timestamp": str(datetime.now()),
            "attr1": random(),
            "msg": f"User SATA, access app-fenix on year 6666"
        }

        str_body = json.dumps(body)
        data = base64.urlsafe_b64encode(bytearray(str_body, 'utf8'))
        publisher.publish(data)
        time.sleep(random() * 8)
        i += 1


if __name__ == "__main__":
    main()