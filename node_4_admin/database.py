import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv
import os

# --- LOAD ENVIRONMENT VARIABLES ---
# This script also needs the .env file to run
load_dotenv() 

# --- YOUR DATABASE CONFIGURATION (NOW SECURE) ---
DB_CONFIG = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASS'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME')
}

# --- FINAL TABLE DEFINITIONS ---
TABLES = {}

# This is the final "topics" schema
TABLES['topics'] = (
    "CREATE TABLE `topics` ("
    "  `topic_id` int(11) NOT NULL AUTO_INCREMENT,"
    "  `topic_name` varchar(255) NOT NULL UNIQUE,"
    "  `status` enum('pending', 'approved', 'active', 'rejected') NOT NULL DEFAULT 'pending',"
    "  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
    # --- The 3 new recipe columns ---
    "  `source_file` varchar(255) NOT NULL,"
    "  `source_column` varchar(255) NOT NULL,"
    "  `output_key` varchar(255) NOT NULL,"
    "  PRIMARY KEY (`topic_id`)"
    ") ENGINE=InnoDB")

# This is the final "user_subscriptions" schema
TABLES['user_subscriptions'] = (
    "CREATE TABLE `user_subscriptions` ("
    "  `subscription_id` int(11) NOT NULL AUTO_INCREMENT,"
    "  `topic_id` int(11) NOT NULL,"
    "  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
    # --- The status column for approvals ---
    "  `status` enum('pending', 'approved', 'rejected') NOT NULL DEFAULT 'pending',"
    "  PRIMARY KEY (`subscription_id`),"
    "  KEY `topic_id` (`topic_id`),"
    "  FOREIGN KEY (`topic_id`) REFERENCES `topics` (`topic_id`) ON DELETE CASCADE"
    ") ENGINE=InnoDB")

# --- SCRIPT TO CREATE THE TABLES ---
def create_tables():
    try:
        # Connect to the database
        print("Connecting to MySQL database...")
        cnx = mysql.connector.connect(**DB_CONFIG)
        cursor = cnx.cursor()
        print("Connected!")

        # Loop over our table definitions and create them
        for table_name in TABLES:
            table_description = TABLES[table_name]
            try:
                print(f"Creating table '{table_name}'... ", end='')
                cursor.execute(table_description)
                print("OK")
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("already exists.")
                else:
                    print(err.msg)

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your username or password in .env")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    finally:
        # Clean up
        if 'cursor' in locals():
            cursor.close()
        if 'cnx' in locals():
            cnx.close()
        print("MySQL connection closed.")

# --- RUN THE SCRIPT ---
if __name__ == "__main__":
    create_tables()