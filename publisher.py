"""Publishes multiple messages to a Pub/Sub topic with an error handler."""
from concurrent import futures
from google.cloud import pubsub_v1
from typing import Callable
import os
import pip._vendor.requests
import time

# Set your Google Cloud project ID and Pub/Sub topic ID
project_id = "ps-int-datateamrnd-22072022"
topic_id = "pubsub-test-lingga"

# service acc credentials
credentials = 'D:\WORK\lingga-sa-int-datateamrnd-22072022-1e041e96c9a0.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials

def publish_message(project_id, topic_id, message):
    # Create a Publisher client
    publisher = pubsub_v1.PublisherClient()

    # Format the topic path
    topic_path = publisher.topic_path(project_id, topic_id)

    # Convert the message to bytes
    data = message.encode('utf-8')

    # Publish the message
    future = publisher.publish(topic_path, data)

    # Wait for the message to be published
    future.result()


# Set the message content
message = '{"message":"another message from local"}'

# Publish the message to Pub/Sub
publish_message(project_id, topic_id, message)
print("message sent")
