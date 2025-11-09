# Node 1: Producer (Quant Model & Provisioning Service)

## Overview

This service is the primary data ingestion and provisioning engine for the "Internal Quant Analytics Marketplace." It is a multi-threaded, **metadata-driven** Python application that acts as the "Quant Team's" trusted service.

This node has two core responsibilities:
1.  **Provisioning (The "Topic Watcher"):** It polls the Admin database (Node 4) for any "data products" (topics) that have been approved. It then uses the `KafkaAdminClient` to programmatically create these topics on the Kafka Broker (Node 2) and updates their status in the DB to "active."
2.  **Publishing (The "Routing Engine"):** It reads raw data from local CSV files (the "Quant Model" input) and publishes it. It uses a dynamic "routing map" (also built from the DB) to know which columns from which files belong to which Kafka topics.

This architecture is "smooth": a new data product can be fully deployed **without ever touching this service's code.**

---

## Core Architecture (Graded Deliverables)

This application is built on three concurrent threads that communicate via thread-safe data structures.

### 1. Input Listener Thread
* **Job:** Reads all specified CSV files (e.g., `AAPL.csv`, `MSFT.csv`) from the local directory.
* **Action:** It reads one row at a time and puts a tuple of `(filename, data_row_dict)` onto a shared `data_queue`.
* **Purpose:** This simulates the continuous, high-velocity intake of raw data for the quant models.

### 2. Topic Watcher Thread
* **Job:** This is the "brain" of the platform. It polls the Node 4 MySQL database every 10 seconds.
* **Action (Provisioning):** It finds all topics with `status = 'approved'`, creates them on the Kafka Broker (Node 2) using the `KafkaAdminClient`, and updates their status in the DB to `'active'`.
* **Action (Routing Logic):** It queries the `topics` table to build a "routing map." This map links a `source_file` and `source_column` (from the DB) to a `product_name` (the Kafka topic). This map is then safely passed to the Publisher thread.

### 3. Publisher Thread
* **Job:** This is a **generic, metadata-driven router**.
* **Action:**
    1.  Pulls a `(filename, data_row_dict)` tuple from the `data_queue`.
    2.  Looks up the `filename` in the "routing map" to get a list of rules.
    3.  For each rule, it dynamically extracts the correct data (e.g., `data_row['RSI_14']`) and publishes it to the correct topic (e.g., `analytics-aapl-rsi`).

---

## Setup & Configuration

This node requires Python 3.9+ and several external dependencies.

### 1. Dependencies
First, create and activate a virtual environment in this folder:

```bash
# Create the virtual environment (only once)
python3 -m venv venv

# Activate it (run this every time you open a new terminal)
source venv/bin/activate

# Install required libraries
pip install -r requirements.txt