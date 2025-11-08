# --- [Node 1: producer.py] ---
# This is the "Quant Model" and "Provisioning Service"
# It reads data, dynamically creates topics, and publishes to them
# based on rules defined in the central database.

import time
import queue
import threading
import pandas as pd
import json
import os
import sys
from dotenv import load_dotenv
import mysql.connector
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

# --- 1. Load Configuration ---
# This reads your .env file
load_dotenv()
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_NAME = os.getenv('DB_NAME')
KAFKA_BROKER = os.getenv('KAFKA_BROKER')

# --- 2. Shared Data Structures (Thread-Safe) ---

# The "conveyor belt" for data from the file reader to the publisher
# Format: (filename, data_row_dict)
data_queue = queue.Queue(maxsize=1000)

# The "routing map" (our "brain").
# This is loaded from the DB by the Watcher and read by the Publisher.
# Format: {"data/AAPL.csv": [{"topic": "...", "source_col": "...", "out_key": "..."}]}
routing_map = {}
# A "Lock" to prevent two threads from accessing the map at the same time
routing_map_lock = threading.Lock()

# --- 3. The 3 Threads (Your Graded Deliverables) ---

def input_listener_thread(files_to_watch: list):
    """
    Input Listener Thread
    - Reads *all* CSV files in a continuous loop.
    - Puts (filename, data_row_dict) tuples onto the shared data_queue.
    """
    print(f"[Input Thread] Started. Watching files: {files_to_watch}")

    # Create a dictionary of Pandas file readers (iterators)
    try:
        # Use the full file path to read
        file_readers = {f: pd.read_csv(f, chunksize=1) for f in files_to_watch}
    except FileNotFoundError as e:
        print(f"[Input Thread] CRITICAL: File not found: {e}.")
        print(f"Make sure CSVs are in the '{os.path.basename(os.path.dirname(files_to_watch[0]))}' directory. Exiting.")
        sys.exit(1) # Exit the whole program if data is missing

    while True:
        # Note: file_name here is the *full path* (e.g., "data/AAPL.csv")
        for file_name, reader in file_readers.items():
            try:
                # Get the next row from the file
                data_row = next(reader).to_dict('records')[0]
                
                # Put the (filename, data_row) tuple on the queue
                # We use os.path.basename to get just "AAPL.csv" as the key
                # This ensures it matches Saniya's database entry.
                data_queue.put((os.path.basename(file_name), data_row))

            except StopIteration:
                # Reached end of file, restart the reader for a continuous stream
                file_readers[file_name] = pd.read_csv(file_name, chunksize=1)
                # print(f"[Input Thread] Restarting stream for {file_name}")
            except Exception as e:
                print(f"[Input Thread] Error reading {file_name}: {e}")
                
        time.sleep(0.1) # Control the overall stream speed (10 rows/sec)

def topic_watcher_thread():
    """
    Topic Watcher Thread
    - Connects to the central DB (Node 4).
    - JOB 1: Finds 'approved' topics, creates them in Kafka (Node 2) via AdminClient,
      and updates their status in the DB to 'active'.
    - JOB 2: Builds the "routing_map" from *all* 'active' topics and shares it
      with the publisher thread.
    """
    print("[Watcher Thread] Started. Polling database...")

    # Connect to Kafka AdminClient (retry until successful)
    while True:
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=KAFKA_BROKER,
                client_id='producer-admin-client'
            )
            print("[Watcher Thread] Connected to Kafka Admin.")
            break
        except NoBrokersAvailable:
            print(f"[Watcher Thread] Kafka Broker at {KAFKA_BROKER} not available. Retrying in 10s...")
            time.sleep(10)

    # Main polling loop
    while True:
        new_routing_map = {}
        try:
            # 1. Connect to Saniya's MySQL DB
            db_conn = mysql.connector.connect(
                host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME
            )
            cursor = db_conn.cursor(dictionary=True)

            # --- JOB 1: Provision New Topics ---
            cursor.execute("SELECT * FROM topics WHERE status = 'approved'")
            approved_topics = cursor.fetchall()

            if approved_topics:
                print(f"[Watcher Thread] Found {len(approved_topics)} topics to provision...")
                new_topics_to_create = []
                for topic in approved_topics:
                    new_topics_to_create.append(
                        NewTopic(name=topic['product_name'], num_partitions=1, replication_factor=1)
                    )
                
                # Create topics in Kafka
                try:
                    admin_client.create_topics(new_topics=new_topics_to_create, validate_only=False)
                    print(f"[Watcher Thread] Successfully created/verified {len(new_topics_to_create)} topics in Kafka.")
                except TopicAlreadyExistsError:
                    print(f"[Watcher Thread] Topics already existed, which is fine.")
                
                # Update DB status to 'active'
                for topic in approved_topics:
                    cursor.execute("UPDATE topics SET status = 'active' WHERE id = %s", (topic['id'],))
                db_conn.commit()

            # --- JOB 2: Build the Routing Map ---
            cursor.execute("SELECT * FROM topics WHERE status = 'active'")
            active_products = cursor.fetchall()

            for product in active_products:
                file = product['source_file'] # e.g., "AAPL.csv"
                rule = {
                    "topic": product['product_name'],
                    "source_col": product['source_column'],
                    "out_key": product['output_key']
                }
                
                if file not in new_routing_map:
                    new_routing_map[file] = []
                new_routing_map[file].append(rule)

            # --- Safely update the shared global routing_map ---
            with routing_map_lock:
                global routing_map
                routing_map = new_routing_map
            
            cursor.close()
            db_conn.close()

        except mysql.connector.Error as e:
            print(f"[Watcher Thread] DB Connection Error: {e}. Is (Node 4) DB running at {DB_HOST}? Retrying in 10s...")
        except Exception as e:
            print(f"[Watcher Thread] Unknown Error: {e}")

        time.sleep(10) # Poll every 10 seconds

def publisher_thread():
    """
    Publisher Thread
    - A generic, metadata-driven router.
    - Gets (filename, data_row) from the queue.
    - Looks up the `routing_map` to find the rules for that file.
    - Applies all rules, dynamically builds, and publishes messages.
    """
    print("[Publisher Thread] Started. Waiting for data...")

    # Connect to Kafka Producer (retry until successful)
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'), # default=str handles NaNs
                client_id='producer-publisher'
            )
            print("[Publisher Thread] Connected to Kafka Producer.")
            break
        except NoBrokersAvailable:
            print(f"[Publisher Thread] Kafka Broker at {KAFKA_BROKER} not available. Retrying in 10s...")
            time.sleep(10)

    # Main processing loop
    while True:
        try:
            # 1. Get the tuple: (filename, data_row)
            # The filename here is just the base name, e.g., "AAPL.csv"
            (source_file, data_row) = data_queue.get()

            # 2. Get the current rules safely
            with routing_map_lock:
                # Get the list of rules for this file, or an empty list
                rules_for_this_file = routing_map.get(source_file, [])
            
            # 3. Apply all rules for this file
            if rules_for_this_file:
                for rule in rules_for_this_file:
                    # e.g., rule = {"topic": "analytics-aapl-rsi", "source_col": "RSI_14", ...}
                    
                    # 4. Dynamically build the message
                    # Use .get() for safety, in case a column is missing
                    message = {
                        "symbol": data_row.get('Symbol'),
                        "date": data_row.get('Date'),
                        rule['out_key']: data_row.get(rule['source_col'])
                    }
                    
                    # 5. Dynamically send it
                    producer.send(rule['topic'], value=message)
            
            # 6. Mark the item as processed
            data_queue.task_done()
            
            # producer.flush() # Optional: flush more often if you need lower latency

        except Exception as e:
            print(f"[Publisher Thread] Error publishing: {e}")
            # Don't re-raise, just log and continue
            if not data_queue.empty():
                data_queue.task_done() # Mark as done even on error to avoid blocking

# --- 4. Main function to start the threads ---
if __name__ == "__main__":
    print("--- [Producer Node 1] Starting Up ---")
    
    # --- !! CONFIGURATION !! ---
    DATA_DIR = "data" # This is your new subdirectory
    
    # Define which CSV files the Input Thread should watch.
    csv_files = [
        "AAPL.csv",
        "GOOGL.csv",
        "MSFT.csv",
        "AMZN.csv",
        "FB.csv",
        "GE.csv",
        "GS.csv",
        "IBM.csv",
        "JPM.csv",
        "TSLA.csv"
    ] 

    # Create the full, correct paths
    FILES_TO_WATCH = [os.path.join(DATA_DIR, f) for f in csv_files]

    # 1. Create the threads
    # daemon=True means these threads will automatically shut down when the main program stops
    listener = threading.Thread(target=input_listener_thread, args=(FILES_TO_WATCH,), daemon=True)
    watcher = threading.Thread(target=topic_watcher_thread, daemon=True)
    publisher = threading.Thread(target=publisher_thread, daemon=True)

    # 2. Start the threads
    listener.start()
    watcher.start()
    publisher.start()

    # 3. Keep the main thread alive (this is the "program")
    try:
        while True:
            time.sleep(30)
            # A simple heartbeat to show the main thread is alive
            with routing_map_lock:
                active_product_count = sum(len(rules) for rules in routing_map.values())
            print(f"[Main Thread] Producer is alive. Queue size: {data_queue.qsize()}. Active Products: {active_product_count}")
            
    except KeyboardInterrupt:
        print("\n--- [Producer Node 1] Shutting Down ---")
        sys.exit(0)