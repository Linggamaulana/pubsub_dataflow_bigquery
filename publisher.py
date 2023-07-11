"""Publishes multiple messages to a Pub/Sub topic with an error handler."""
from concurrent import futures
from google.cloud import pubsub_v1
from typing import Callable
import os
import pip._vendor.requests
import time

# Set your Google Cloud project ID and Pub/Sub topic ID
project_id = "your project id" #replace with your project id
topic_id = "your topic id" #Replace with your topic id

# service acc credentials
credentials = "your service account path/directory" #Replace with your service account path/directory
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
message = '{"message":"another message from local"}' #write your message here

# Publish the message to Pub/Sub
publish_message(project_id, topic_id, message)
print("message sent")
