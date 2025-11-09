# --- [Node 3: consumer.py] ---
# UPDATED to use environment variables for security.

import time
import queue
import threading
import json
import os
import sys
import mysql.connector
import requests
import csv
import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv  # <-- NEW: Load .env file

# --- 1. Load Configuration from Environment Variables ---
load_dotenv()  # <-- NEW: Load variables from .env file

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_NAME = os.getenv('DB_NAME')
ADMIN_API_URL = os.getenv('ADMIN_API_URL')
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
POLL_INTERVAL_SECONDS = int(os.getenv('POLL_INTERVAL_SECONDS', 10))

# --- NEW: Check if all variables are loaded ---
REQUIRED_VARS = [DB_HOST, DB_USER, DB_PASS, DB_NAME, ADMIN_API_URL, KAFKA_BROKER]
if not all(REQUIRED_VARS):
    print("CRITICAL ERROR: One or more environment variables are not set.")
    print("Please check your .env file.")
    sys.exit(1)

# --- Configuration for Data Storage ---
current_time_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
DATA_FILE = f"consumed_stock_data_{current_time_str}.csv"
DATA_HEADER = ['topic', 'symbol', 'price', 'volume', 'timestamp']


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

# --- 3. The 2 Threads ---

def get_active_topics_from_api():
    """Polls the Admin's Flask API (Node 4) for topics."""
    try:
        response = requests.get(ADMIN_API_URL)
        response.raise_for_status() 
        data = response.json() 
        topic_list = data.get("subscribed_topics", [])
        return set(topic_list)
        
    except Exception as e:
        print(f"[Watcher] ERROR: Could not get topics from Admin API at {ADMIN_API_URL}: {e}")
        return set()

def topic_watcher_thread(consumer_state: ConsumerState, consumer_instance: KafkaConsumer):
    """
    Topic Watcher Thread (Consumer)
    - Polls the Admin API (Node 4) for 'active' and 'approved' topics.
    - Dynamically updates the main consumer thread's subscriptions.
    - Prints explicit add/remove log messages.
    """
    print(f"[Watcher Thread] Started. Polling Admin API at {ADMIN_API_URL}...")
    current_local_subs = set() 
    
    while True: 
        try:
            new_subs_from_api = get_active_topics_from_api()

            if new_subs_from_api != current_local_subs:
                print(f"[Watcher Thread] --- DYNAMIC UPDATE: Subscriptions changed! ---")
                
                added_topics = new_subs_from_api - current_local_subs
                removed_topics = current_local_subs - new_subs_from_api

                if removed_topics:
                    print(f"[Watcher Thread] ==> Unsubscribed from: {removed_topics}")
                if added_topics:
                    print(f"[Watcher Thread] ==> Subscribed to: {added_topics}")
                
                consumer_state.set_subscriptions(new_subs_from_api)
                
                if new_subs_from_api:
                    print(f"[Watcher Thread] New active subscription list: {new_subs_from_api}")
                    consumer_instance.subscribe(list(new_subs_from_api))
                else:
                    print("[Watcher Thread] No active topics. Unsubscribing from all.")
                    consumer_instance.unsubscribe()
                
                current_local_subs = new_subs_from_api.copy()

        except Exception as e:
            print(f"[Watcher Thread] Unknown Error: {e}")
        
        time.sleep(POLL_INTERVAL_SECONDS)

# --- 4. Data Processing & Storage ---

def connect_to_db():
    """Connects to the MySQL database, retrying on failure."""
    while True:
        try:
            print("[Consumer Thread] Connecting to MySQL DB...")
            # --- UPDATED: Use env vars ---
            conn = mysql.connector.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASS,
                database=DB_NAME
            )
            print("[Consumer Thread] Connected to MySQL DB.")
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    topic VARCHAR(255),
                    symbol VARCHAR(50),
                    price DECIMAL(10, 2),
                    volume INT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.close()
            return conn
        except mysql.connector.Error as e:
            print(f"[Consumer Thread] DB connection failed: {e}. Retrying in 10s...")
            time.sleep(10)

def process_message_to_db(db_connection, topic, data):
    """Saves the processed message to the MySQL database."""
    try:
        symbol = data.get('symbol', 'UNKNOWN')
        price = data.get('price', 0)
        volume = data.get('volume', 0)
        
        sql = "INSERT INTO trades (topic, symbol, price, volume) VALUES (%s, %s, %s, %s)"
        values = (topic, symbol, price, volume)
        
        cursor = db_connection.cursor()
        cursor.execute(sql, values)
        db_connection.commit()
        cursor.close()
        
        print(f"  [DB] Successfully processed and saved: {symbol} @ {price}")

    except mysql.connector.Error as e:
        print(f"  [DB] ERROR: Failed to insert record: {e}")
        db_connection.rollback()
        raise 

def save_data_to_csv(topic, data):
    """Saves the raw message data to a CSV file."""
    try:
        file_exists = os.path.isfile(DATA_FILE)
        
        with open(DATA_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=DATA_HEADER)
            if not file_exists:
                writer.writeheader()
            
            row = {
                'topic': topic,
                'symbol': data.get('symbol', 'N/A'),
                'price': data.get('price', 0),
                'volume': data.get('volume', 0),
                'timestamp': data.get('timestamp', time.time())
            }
            writer.writerow(row)
            
        print(f"  [CSV] Successfully saved data to {DATA_FILE}")
            
    except Exception as e:
        print(f"  [CSV] ERROR: Failed to save to CSV: {e}")
        raise

# --- 5. Main Consumer Loop ---

def start_consumer_loop():
    """
    Consumer Thread (Main)
    - Connects to Kafka.
    - Connects to MySQL DB.
    - Starts the Topic Watcher thread.
    - Enters the main consumption loop with MANUAL offset commits.
    """
    print("[Consumer Thread] Started. Connecting to Kafka...")
    
    while True:
        try:
            # --- UPDATED: Use env var ---
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset='earliest',
                group_id='quant-trading-desk-consumers',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                client_id='consumer-main',
                enable_auto_commit=False 
            )
            print(f"[Consumer Thread] Connected to Kafka Broker at {KAFKA_BROKER}.")
            break
        except NoBrokersAvailable:
            print(f"[Consumer Thread] Kafka Broker at {KAFKA_BROKER} not available. Retrying...")
            time.sleep(10)

    print(f"[Consumer Thread] Logging consumed data to: {DATA_FILE}")

    db_conn = connect_to_db()

    state = ConsumerState()
    watcher = threading.Thread(
        target=topic_watcher_thread, 
        args=(state, consumer), 
        daemon=True
    )
    watcher.start()

    print("[Consumer Thread] Now listening for messages...")
    try:
        for message in consumer:
            print(f"\n[Consumer] ==> Message Received!")
            print(f"  Topic:     {message.topic}")
            print(f"  Data:      {message.value}")
            
            try:
                process_message_to_db(db_conn, message.topic, message.value)
                save_data_to_csv(message.topic, message.value)
                
                consumer.commit() 
                print("  [Kafka] Offset committed successfully.")

            except Exception as e:
                print(f"  [!] CRITICAL: Failed to process message: {e}")
                print("  [!] Not committing offset. Message will be retried on next poll.")

    except KeyboardInterrupt:
        print("\n--- [Consumer Node 3] Shutting Down ---")
    finally:
        consumer.close()
        if db_conn and db_conn.is_connected():
            db_conn.close()
            print("[Consumer Thread] MySQL connection closed.")
        print("[Consumer Thread] Consumer has been closed.")

# --- 6. Main function to start the consumer ---
if __name__ == "__main__":
    # You MUST install the 'dotenv' library for this to work!
    # pip install python-dotenv
    print("--- [Consumer Node 3] Starting Up ---")
    start_consumer_loop()
