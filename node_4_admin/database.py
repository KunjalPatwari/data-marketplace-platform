import mysql.connector
from mysql.connector import errorcode

# --- YOUR DATABASE CONFIGURATION ---
# IMPORTANT: Replace with the passwords you created!
DB_CONFIG = {
    'user': 'flask_app',
    'password': 'Flask@123',  # <-- REPLACE THIS
    'host': 'localhost',
    'database': 'marketplace_db'
}

# --- TABLE DEFINITIONS ---
TABLES = {}

TABLES['topics'] = (
    "CREATE TABLE `topics` ("
    "  `topic_id` int(11) NOT NULL AUTO_INCREMENT,"
    "  `topic_name` varchar(255) NOT NULL UNIQUE,"
    "  `status` enum('pending', 'approved', 'active', 'rejected') NOT NULL DEFAULT 'pending',"
    "  `owner_user` varchar(100) NOT NULL,"
    "  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
    "  PRIMARY KEY (`topic_id`)"
    ") ENGINE=InnoDB")

TABLES['user_subscriptions'] = (
    "CREATE TABLE `user_subscriptions` ("
    "  `subscription_id` int(11) NOT NULL AUTO_INCREMENT,"
    "  `user_name` varchar(100) NOT NULL,"
    "  `topic_id` int(11) NOT NULL,"
    "  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
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
            print("Something is wrong with your username or password")
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