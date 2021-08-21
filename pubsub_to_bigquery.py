from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.auth import jwt
import json
from google.cloud import bigquery
import base64
from datetime import datetime

project_id = "[PROJECT_ID]"
subscription_id = "[SUBSCRIPTION_ID]"
# Number of seconds the subscriber should listen for messages
timeout = 100.0

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)


def write_tweets_to_bq(dataset_id, table_id, tweets):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref)

    errors = client.insert_rows(table, tweets)
    if not errors:
        print('Loaded {} row(s) into {}:{}'.format(len(tweets), dataset_id, table_id))
    else:
        print('Errors:')
        for error in errors:
            print(error)


def collect_messages(data):
    stream = base64.urlsafe_b64decode(data)
    raw = json.loads(stream)
    message = (datetime.now(), raw.get('attr1'), raw.get('msg'))

    write_tweets_to_bq('test', 'example', [message])


def callback(message):
    print(f"\nReceived {message.data}")
    collect_messages(message.data)
    message.ack()


streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...\n")


# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.


