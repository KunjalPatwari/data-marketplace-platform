# Node 4: Admin & Control Plane

This directory contains all the code for **Node 4**, which acts as the central brain, control panel, and user interface for the entire Data Marketplace.

This is a **Flask** web application that provides:
* A **MySQL** database as the single source of truth for all topics and subscriptions.
* A **Web UI** for producers to register topics and for consumers to browse and subscribe.
* A private **Admin Dashboard** for approving or rejecting all requests.
* A simple **JSON API** that the Consumer (Node 3) script uses to get its list of approved topics.

This node **does not** handle any Kafka data streaming. It only manages the *metadata and permissions* that enable the other nodes to function.

## 🚀 Key Features

* **Marketplace (`/`):** A public "storefront" that displays all `active` topics for the Consumer to browse.
* **Producer Portal (`/register`):** A private link/form for the Producer (Node 1) to submit a new topic "recipe" (source file, column, and key) for approval.
* **Consumer Dashboard (`/my_subscriptions`):** A private dashboard for the Consumer (Node 3) to see the status (`pending`, `approved`, `rejected`) of all their subscription requests.
* **Admin Panel (`/admin`):** A private dashboard for the Admin (Node 4) to view and approve/reject all pending topic and subscription requests.
* **Consumer API (`/api/subscriptions`):** A JSON endpoint for the Consumer's script to poll, which returns a simple list of all topics they are approved to read.

## 🔧 How to Run This Node

Follow these steps to set up and run the Node 4 server on your VM.

### 1. Prerequisites

* Ubuntu VM (or similar Linux) with ZeroTier installed.
* Python 3.10+
* MySQL Server
* Git

### 2. Database Setup (One-Time)

This node requires a MySQL database and a dedicated user.

1.  **Secure MySQL & Set Root Password:**
    ```bash
    sudo mysql_secure_installation
    ```
2.  **Log in to MySQL as root:**
    ```bash
    mysql -u root -p
    ```
3.  **Create the database, local user, and remote user:** (Run these inside the `mysql>` prompt)
    ```sql
    -- Create the database
    CREATE DATABASE marketplace_db;

    -- Create the user for the app to use on 'localhost'
    CREATE USER 'flask_app'@'localhost' IDENTIFIED BY 'Flask@123';
    GRANT ALL PRIVILEGES ON marketplace_db.* TO 'flask_app'@'localhost';

    -- Create the user for your teammates to use remotely
    CREATE USER 'flask_app'@'%' IDENTIFIED BY 'Flask@123';
    GRANT ALL PRIVILEGES ON marketplace_db.* TO 'flask_app'@'%';

    -- Apply all changes
    FLUSH PRIVILEGES;
    EXIT;
    ```
4.  **Allow Remote Connections:**
    * Open the MySQL config file:
        ```bash
        sudo nano /etc/mysql/mysql.conf.d/mysqld.cnf
        ```
    * Find the line `bind-address = 127.0.0.1` and change it to:
        ```ini
        bind-address = 0.0.0.0
        ```
    * Save (`Ctrl+O`), Exit (`Ctrl+X`), and restart MySQL:
        ```bash
        sudo systemctl restart mysql
        ```

### 3. Application Setup

1.  **Clone the Repo (if not done):**
    ```bash
    git clone [https://github.com/KunjalPatwari/data-marketplace-platform.git](https://github.com/KunjalPatwari/data-marketplace-platform.git)
    cd data-marketplace-platform
    ```
2.  **Set up Python Environment:**
    ```bash
    # Create and activate a virtual environment
    python3 -m venv venv
    source venv/bin/activate
    
    # Install this node's requirements
    pip install -r node_4_admin/requirements.txt
    ```
3.  **Create Your `.env` File:**
    * Navigate into the admin folder: `cd node_4_admin`
    * Create the file: `touch .env`
    * Open the file (`nano .env`) and paste the following:
        ```ini
        DB_HOST="localhost"
        DB_USER="flask_app"
        DB_PASS="Flask@123"
        DB_NAME="marketplace_db"
        ```
4.  **Create Database Tables:**
    * From the `data-marketplace-platform` (root) folder, run the `database.py` script:
        ```bash
        python3 node_4_admin/database.py
        ```
    * You should see "tables created" messages.

### 4. Run the Server

1.  **Navigate to the admin folder:**
    ```bash
    cd node_4_admin
    ```
2.  **Run the Flask app:**
    ```bash
    python3 app.py
    ```
3.  The server is now running and accessible to your team at **`http://192.168.196.197:5000`**.

## 🔌 Information for Other Nodes

### For Node 1 (Producer)

1.  **Web Portal:** To register new topics, go to:
    * `http://192.168.196.197:5000/register`
2.  **Database Config:** Your script needs to connect to the database. Add this to your `.env` file:
    ```ini
    DB_HOST="192.168.196.197"
    DB_USER="flask_app"
    DB_PASS="Flask@123"
    DB_NAME="marketplace_db"
    ```
3.  **Your Script's Logic:**
    * **Watch:** Poll the database for topics where `status = 'approved'`.
    * **Act:** When found, create the topic in Kafka.
    * **Update:** Immediately after, run this SQL query:
        ```sql
        UPDATE topics SET status = 'active' WHERE topic_id = ?;
        ```

### For Node 3 (Consumer)

1.  **Web Portals:**
    * **Browse Topics:** `http://192.168.196.197:5000/`
    * **Check Your Status:** `http://192.168.196.197:5000/my_subscriptions`
2.  **Consumer API:** Your script does **not** connect to the database. It must poll this API URL in a loop:
    ```
    [http://192.168.196.197:5000/api/subscriptions](http://192.168.196.197:5000/api/subscriptions)
    ```
3.  **API Response:**
    * It will return a JSON object. Your script only needs to read the `subscribed_topics` list.
    * **Example:**
        ```json
        {
          "subscribed_topics": ["analytics-GOOG-RSI", "analytics-VIX-scores"]
        }
        ```

## 🗄️ Database Schema

### `topics` Table

This table stores the "recipes" for all data products.

| Column | Type | Description |
| :--- | :--- | :--- |
| `topic_id` | `int(11)` | Primary Key |
| `topic_name` | `varchar(255)` | The name of the Kafka topic (e.g., `analytics-AAPL-RSI`) |
| `status` | `enum(...)` | `pending`, `approved`, `active`, or `rejected` |
| `source_file` | `varchar(255)` | The source filename (e.g., `AAPL.csv`) |
| `source_column`| `varchar(255)` | The column to read from (e.g., `RSI_14`) |
| `output_key` | `varchar(255)` | The key for the final JSON message (e.g., `rsi_value`) |
| `created_at` | `timestamp` | When the request was made |

### `user_subscriptions` Table

This table stores all subscription requests.

| Column | Type | Description |
| :--- | :--- | :--- |
| `subscription_id` | `int(11)` | Primary Key |
| `topic_id` | `int(11)` | Foreign Key to `topics.topic_id` |
| `status` | `enum(...)` | `pending`, `approved`, or `rejected` |
| `created_at` | `timestamp` | When the request was made |
