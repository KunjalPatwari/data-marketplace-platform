# --- [Node 3: consumer.py] ---
# Trading Bot / Dynamic Kafka Consumer
# Now uses environment variables for all sensitive config.

import time
import queue
import threading
import json
import os
import sys
import requests
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# --- 1. Load Configuration from Environment Variables ---
# Use defaults only for non-sensitive values

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_USER = os.getenv("DB_USER", "flask_app")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "marketplace_db")

ADMIN_API_URL = os.getenv("ADMIN_API_URL", "http://127.0.0.1:5000/api/subscriptions")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:9092")

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "10"))


# --- 2. Shared Data Structures (Thread-Safe) ---
class ConsumerState:
    def __init__(self):
        self.subscriptions = set()
        self.lock = threading.Lock()

    def set_subscriptions(self, new_subscriptions: set):
        with self.lock:
            if new_subscriptions != self.subscriptions:
                self.subscriptions = new_subscriptions
                return True
            return False


# --- 3. Topic fetch from Admin API ---

def get_active_topics_from_api():
    try:
        response = requests.get(ADMIN_API_URL, timeout=5)
        response.raise_for_status()
        data = response.json()
        topic_list = data.get("subscribed_topics", [])
        return set(topic_list)

    except Exception as e:
        print(f"[Watcher] ERROR contacting admin API {ADMIN_API_URL}: {e}")
        return set()


def topic_watcher_thread(consumer_state: ConsumerState, consumer_instance: KafkaConsumer):
    print(f"[Watcher Thread] Polling Admin API at {ADMIN_API_URL}...")

    while True:
        try:
            active_topics = get_active_topics_from_api()

            if consumer_state.set_subscriptions(active_topics):
                print(f"[Watcher Thread] Subscription set changed → {active_topics}")

                if active_topics:
                    consumer_instance.subscribe(list(active_topics))
                else:
                    print("[Watcher Thread] No active topics; unsubscribing.")
                    consumer_instance.unsubscribe()

        except Exception as e:
            print(f"[Watcher Thread] ERROR: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


# --- Main Consumer Loop ---

def start_consumer_loop():
    print("[Consumer] Connecting to Kafka...")

    while True:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BROKER,
                group_id='quant-trading-desk-consumers',
                auto_offset_reset='earliest',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                client_id='consumer-main',
                enable_auto_commit=True
            )
            print(f"[Consumer] Connected to {KAFKA_BROKER}")
            break

        except NoBrokersAvailable:
            print(f"[Consumer] Broker {KAFKA_BROKER} unavailable. Retrying...")
            time.sleep(5)

    state = ConsumerState()
    watcher = threading.Thread(
        target=topic_watcher_thread,
        args=(state, consumer),
        daemon=True
    )
    watcher.start()

    print("[Consumer] Waiting for messages...")

    try:
        for message in consumer:
            print(f"\n[Consumer] Message received on '{message.topic}' → {message.value}")

    except KeyboardInterrupt:
        print("\n[Consumer] Shutting down...")

    finally:
        consumer.close()
        print("[Consumer] Closed cleanly.")


if __name__ == "__main__":
    print("--- [Consumer Node 3] Starting Up ---")
    start_consumer_loop()
