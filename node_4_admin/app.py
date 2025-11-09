from flask import Flask, render_template, request, redirect, url_for, session
import mysql.connector
from mysql.connector import errorcode
import os 
from dotenv import load_dotenv # <-- 1. Import load_dotenv

# --- LOAD ENVIRONMENT VARIABLES ---
load_dotenv() # <-- 2. Call this to load the .env file

# --- FLASK APP SETUP ---
app = Flask(__name__)
app.secret_key = os.urandom(24) 

# --- YOUR DATABASE CONFIGURATION (NOW SECURE) ---
# 3. Read everything from os.getenv()
DB_CONFIG = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASS'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME')
}

# --- DATABASE CONNECTION HELPER ---
def get_db_connection():
    """Establishes a connection to the database."""
    try:
        cnx = mysql.connector.connect(**DB_CONFIG)
        return cnx
    except mysql.connector.Error as err:
        print(f"Error connecting to database: {err}")
        return None

# --- HOME PAGE / MARKETPLACE ROUTE ---
@app.route("/")
def marketplace():
    """
    Displays all ACTIVE topics. This is the 'dumb' public storefront.
    """
    active_products = []
    cnx = get_db_connection()
    if cnx:
        cursor = cnx.cursor(dictionary=True)
        query = "SELECT topic_id, topic_name FROM topics WHERE status = 'active'"
        cursor.execute(query)
        active_products = cursor.fetchall()
        
        cursor.close()
        cnx.close()
    
    return render_template('index.html', products=active_products)

# --- REGISTER TOPIC ROUTES (GET and POST) ---
@app.route("/register", methods=['GET', 'POST'])
def register_topic():
    if request.method == 'GET':
        return render_template('register.html')
    
    if request.method == 'POST':
        topic_name = request.form['topic_name']
        source_file = request.form['source_file']
        source_column = request.form['source_column']
        output_key = request.form['output_key']
        
        cnx = get_db_connection()
        if cnx:
            cursor = cnx.cursor()
            query = """
                INSERT INTO topics (topic_name, status, source_file, source_column, output_key) 
                VALUES (%s, 'pending', %s, %s, %s)
            """
            try:
                cursor.execute(query, (topic_name, source_file, source_column, output_key))
                cnx.commit()
            except mysql.connector.Error as err:
                print(f"Error registering topic: {err}")
            
            cursor.close()
            cnx.close()
        
        return render_template('register_success.html')

# --- SUBSCRIBE TO TOPIC ROUTES (GET and POST) ---
@app.route("/subscribe/<int:topic_id>", methods=['GET', 'POST'])
def subscribe_to_topic(topic_id):
    cnx = get_db_connection()
    if not cnx:
        return "Database connection failed", 500
    
    cursor = cnx.cursor(dictionary=True)
    
    if request.method == 'POST':
        insert_query = "INSERT INTO user_subscriptions (topic_id, status) VALUES (%s, 'pending')"
        try:
            cursor.execute(insert_query, (topic_id,))
            cnx.commit()
            new_subscription_id = cursor.lastrowid
            session['last_sub_id'] = new_subscription_id
        except mysql.connector.Error as err:
            print(f"Error subscribing: {err}")
        
        cursor.close()
        cnx.close()
        
        return redirect(url_for('my_subscriptions'))

    if request.method == 'GET':
        query = "SELECT topic_id, topic_name FROM topics WHERE topic_id = %s AND status = 'active'"
        cursor.execute(query, (topic_id,))
        product = cursor.fetchone()
        
        cursor.close()
        cnx.close()
        
        if product:
            return render_template('subscribe.html', product=product)
        else:
            return "This product is not available for subscription.", 404

# --- *** UPDATED *** CONSUMER DASHBOARD PAGE ---
@app.route("/my_subscriptions")
def my_subscriptions():
    """
    This is the consumer's dedicated dashboard.
    It shows their LATEST pending, approved, and rejected subscriptions.
    """
    pending_subs = []
    approved_subs = []
    rejected_subs = []

    cnx = get_db_connection()
    if cnx:
        cursor = cnx.cursor(dictionary=True)
        # THIS IS THE NEW, SMARTER QUERY.
        # It finds the *most recent* subscription for each topic
        # and joins to get only that single, latest status.
        query = """
            SELECT T.topic_name, S.status
            FROM user_subscriptions AS S
            JOIN topics AS T ON S.topic_id = T.topic_id
            INNER JOIN (
                SELECT topic_id, MAX(subscription_id) AS max_id
                FROM user_subscriptions
                GROUP BY topic_id
            ) AS S_latest ON S.topic_id = S_latest.topic_id AND S.subscription_id = S_latest.max_id
            ORDER BY S.subscription_id DESC
        """
        
        try:
            cursor.execute(query)
            all_subs = cursor.fetchall()
            
            # Sort all subscriptions into three lists
            for sub in all_subs:
                if sub['status'] == 'pending':
                    pending_subs.append(sub)
                elif sub['status'] == 'approved':
                    approved_subs.append(sub)
                elif sub['status'] == 'rejected':
                    rejected_subs.append(sub)
            
            cursor.close()
            cnx.close()
        
        except mysql.connector.Error as err:
            print(f"my_subscriptions error: {err}")
            cursor.close()
            cnx.close()

    
    return render_template('my_subscriptions.html', 
                           pending_subs=pending_subs,
                           approved_subs=approved_subs,
                           rejected_subs=rejected_subs)


# --- API ENDPOINT FOR CONSUMER SCRIPT (NODE 3) ---
@app.route("/api/subscriptions")
def get_subscriptions():
    topic_list = []
    cnx = get_db_connection()
    if cnx:
        cursor = cnx.cursor(dictionary=True)
        query = """
            SELECT T.topic_name
            FROM topics AS T
            JOIN user_subscriptions AS S ON T.topic_id = S.topic_id
            WHERE T.status = 'active' AND S.status = 'approved'
        """
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            topic_list = [row['topic_name'] for row in results]
        except mysql.connector.Error as err:
            print(f"API Error: {err}")
            
        cursor.close()
        cnx.close()
    
    return { "subscribed_topics": topic_list }

# --- ADMIN DASHBOARD ROUTE ---
@app.route("/admin")
def admin_dashboard():
    pending_topics = []
    live_topics = []
    rejected_topics = []
    
    cnx = get_db_connection()
    if cnx:
        cursor = cnx.cursor(dictionary=True)
        
        query_pending = "SELECT * FROM topics WHERE status = 'pending' ORDER BY created_at DESC"
        cursor.execute(query_pending)
        pending_topics = cursor.fetchall()
        
        query_live = "SELECT * FROM topics WHERE status IN ('approved', 'active') ORDER BY created_at DESC"
        cursor.execute(query_live)
        live_topics = cursor.fetchall()
        
        query_rejected = "SELECT * FROM topics WHERE status = 'rejected' ORDER BY created_at DESC"
        cursor.execute(query_rejected)
        rejected_topics = cursor.fetchall()
        
        cursor.close()
        cnx.close()
    
    return render_template('admin.html', 
                           pending_topics=pending_topics, 
                           live_topics=live_topics, 
                           rejected_topics=rejected_topics)
    
# --- (All Admin Approve/Reject routes remain the same) ---
@app.route("/approve/<int:topic_id>", methods=['POST'])
def approve_topic(topic_id):
    cnx = get_db_connection()
    if cnx:
        cursor = cnx.cursor()
        query = "UPDATE topics SET status = 'approved' WHERE topic_id = %s"
        cursor.execute(query, (topic_id,))
        cnx.commit()
        cursor.close()
        cnx.close()
    return redirect(url_for('admin_dashboard'))

@app.route("/reject_topic/<int:topic_id>", methods=['POST'])
def reject_topic(topic_id):
    cnx = get_db_connection()
    if cnx:
        cursor = cnx.cursor()
        query = "UPDATE topics SET status = 'rejected' WHERE topic_id = %s"
        cursor.execute(query, (topic_id,))
        cnx.commit()
        cursor.close()
        cnx.close()
    return redirect(url_for('admin_dashboard'))

@app.route("/admin/subscriptions")
def admin_subscriptions():
    pending_subs = []
    approved_subs = []
    rejected_subs = []
    
    query_base = """
        SELECT S.subscription_id, T.topic_name
        FROM user_subscriptions AS S
        JOIN topics AS T ON S.topic_id = T.topic_id
    """
    
    cnx = get_db_connection()
    if cnx:
        cursor = cnx.cursor(dictionary=True)
        cursor.execute(f"{query_base} WHERE S.status = 'pending'")
        pending_subs = cursor.fetchall()
        cursor.execute(f"{query_base} WHERE S.status = 'approved'")
        approved_subs = cursor.fetchall()
        cursor.execute(f"{query_base} WHERE S.status = 'rejected'")
        rejected_subs = cursor.fetchall()
        cursor.close()
        cnx.close()
    
    return render_template('admin_subs.html', 
                           pending_subs=pending_subs, 
                           approved_subs=approved_subs, 
                           rejected_subs=rejected_subs)

@app.route("/approve_subscription/<int:subscription_id>", methods=['POST'])
def approve_subscription(subscription_id):
    cnx = get_db_connection()
    if cnx:
        cursor = cnx.cursor()
        query = "UPDATE user_subscriptions SET status = 'approved' WHERE subscription_id = %s"
        cursor.execute(query, (subscription_id,))
        cnx.commit()
        cursor.close()
        cnx.close()
    return redirect(url_for('admin_subscriptions'))

@app.route("/reject_subscription/<int:subscription_id>", methods=['POST'])
def reject_subscription(subscription_id):
    cnx = get_db_connection()
    if cnx:
        cursor = cnx.cursor()
        query = "UPDATE user_subscriptions SET status = 'rejected' WHERE subscription_id = %s"
        cursor.execute(query, (subscription_id,))
        cnx.commit()
        cursor.close()
        cnx.close()
    return redirect(url_for('admin_subscriptions'))

# --- RUN THE APP ---
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)