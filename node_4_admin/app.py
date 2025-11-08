from flask import Flask, render_template, request, redirect, url_for, session
import mysql.connector
from mysql.connector import errorcode
import os # We need this for the new session feature

# --- FLASK APP SETUP ---
app = Flask(__name__)
# A secret key is required for sessions (which we use for the new feature)
app.secret_key = os.urandom(24) 

# --- YOUR DATABASE CONFIGURATION ---
DB_CONFIG = {
    'user': 'flask_app',
    'password': 'Flask@123',
    'host': 'localhost',
    'database': 'marketplace_db'
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
# --- HOME PAGE / MARKETPLACE ROUTE ---
@app.route("/")
def marketplace():
    """
    Displays all ACTIVE topics and checks the LATEST subscription
    status for each one.
    """
    products_list = []
    cnx = get_db_connection()
    if cnx:
        cursor = cnx.cursor(dictionary=True)
        
        # This new, "smart" query joins topics with their LATEST subscription status
        query = """
            SELECT
                T.topic_id,
                T.topic_name,
                (SELECT S.status 
                 FROM user_subscriptions AS S 
                 WHERE S.topic_id = T.topic_id 
                 ORDER BY S.subscription_id DESC 
                 LIMIT 1) AS subscription_status
            FROM topics AS T
            WHERE T.status = 'active'
        """
        
        cursor.execute(query)
        products_list = cursor.fetchall()
        cursor.close()
        cnx.close()
    
    return render_template('index.html', products=products_list)
    
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
    
    # --- THIS IS THE FIX ---
    # When the user *submits* the form
    if request.method == 'POST':
        insert_query = "INSERT INTO user_subscriptions (topic_id, status) VALUES (%s, 'pending')"
        try:
            cursor.execute(insert_query, (topic_id,))
            cnx.commit()
            
            # **NEW:** Get the ID of the subscription we just created
            new_subscription_id = cursor.lastrowid
            
            # **NEW:** Store this ID in the user's session (like a cookie)
            session['last_sub_id'] = new_subscription_id
            
        except mysql.connector.Error as err:
            print(f"Error subscribing: {err}")
        
        cursor.close()
        cnx.close()
        
        # **THE FIX:** We redirect to a new "status" page instead of
        # rendering a template directly. This changes the URL.
        return redirect(url_for('subscription_status'))

    # --- This part handles showing the form ---
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

# --- *** NEW FEATURE ***: SUBSCRIPTION STATUS PAGE ---
@app.route("/subscription_status")
def subscription_status():
    """
    Shows the consumer the status of their *last* subscription request.
    This is where we hide/show the API link.
    """
    # 1. Get the subscription ID we saved in the session
    sub_id = session.get('last_sub_id', None)
    if not sub_id:
        # If they haven't subscribed, just send them home
        return redirect(url_for('marketplace'))

    # 2. Check the status of this *specific* subscription
    status = 'pending' # Default
    
    cnx = get_db_connection()
    if cnx:
        cursor = cnx.cursor(dictionary=True)
        query = "SELECT status FROM user_subscriptions WHERE subscription_id = %s"
        cursor.execute(query, (sub_id,))
        result = cursor.fetchone()
        if result:
            status = result['status'] # Will be 'pending', 'approved', or 'rejected'
        cursor.close()
        cnx.close()

    # 3. Pass this *correct* status to the template
    return render_template('subscribe_success.html', status=status)

# --- API ENDPOINT FOR CONSUMER SCRIPT (NODE 3) ---
@app.route("/api/subscriptions")
def get_subscriptions():
    """
    Returns a JSON list of ALL active and approved topics.
    """
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
    cnx = get_db_connection()
    if cnx:
        cursor = cnx.cursor(dictionary=True)
        query = """
            SELECT topic_id, topic_name, created_at, 
                   source_file, source_column, output_key 
            FROM topics 
            WHERE status = 'pending'
        """
        cursor.execute(query)
        pending_topics = cursor.fetchall()
        
        cursor.close()
        cnx.close()
    
    return render_template('admin.html', topics=pending_topics)
    
# --- APPROVE TOPIC ROUTE (THE "BUTTON") ---
@app.route("/approve/<int:topic_id>", methods=['POST'])
def approve_topic(topic_id):
    cnx = get_db_connection()
    if cnx:
        cursor = cnx.cursor()
        query = "UPDATE topics SET status = 'approved' WHERE topic_id = %s"
        try:
            cursor.execute(query, (topic_id,))
            cnx.commit()
            print(f"Topic {topic_id} approved!")
        except mysql.connector.Error as err:
            print(f"Error approving topic: {err}")
        
        cursor.close()
        cnx.close()
    
    return redirect(url_for('admin_dashboard'))

# --- REJECT TOPIC ROUTE ---
@app.route("/reject_topic/<int:topic_id>", methods=['POST'])
def reject_topic(topic_id):
    cnx = get_db_connection()
    if cnx:
        cursor = cnx.cursor()
        query = "UPDATE topics SET status = 'rejected' WHERE topic_id = %s"
        try:
            cursor.execute(query, (topic_id,))
            cnx.commit()
            print(f"Topic {topic_id} REJECTED!")
        except mysql.connector.Error as err:
            print(f"Error rejecting topic: {err}")
        
        cursor.close()
        cnx.close()
    
    return redirect(url_for('admin_dashboard'))

# --- ADMIN SUBSCRIPTION APPROVAL ROUTES ---
@app.route("/admin/subscriptions")
def admin_subscriptions():
    pending_subs = []
    cnx = get_db_connection()
    if cnx:
        cursor = cnx.cursor(dictionary=True)
        query = """
            SELECT S.subscription_id, T.topic_name
            FROM user_subscriptions AS S
            JOIN topics AS T ON S.topic_id = T.topic_id
            WHERE S.status = 'pending'
        """
        cursor.execute(query)
        pending_subs = cursor.fetchall()
        cursor.close()
        cnx.close()
    
    return render_template('admin_subs.html', subscriptions=pending_subs)

@app.route("/approve_subscription/<int:subscription_id>", methods=['POST'])
def approve_subscription(subscription_id):
    cnx = get_db_connection()
    if cnx:
        cursor = cnx.cursor()
        query = "UPDATE user_subscriptions SET status = 'approved' WHERE subscription_id = %s"
        try:
            cursor.execute(query, (subscription_id,))
            cnx.commit()
            print(f"Subscription {subscription_id} approved!")
        except mysql.connector.Error as err:
            print(f"Error approving subscription: {err}")
        
        cursor.close()
        cnx.close()
    
    return redirect(url_for('admin_subscriptions'))

# --- REJECT SUBSCRIPTION ROUTE ---
@app.route("/reject_subscription/<int:subscription_id>", methods=['POST'])
def reject_subscription(subscription_id):
    cnx = get_db_connection()
    if cnx:
        cursor = cnx.cursor()
        query = "UPDATE user_subscriptions SET status = 'rejected' WHERE subscription_id = %s"
        try:
            cursor.execute(query, (subscription_id,))
            cnx.commit()
            print(f"Subscription {subscription_id} REJECTED!")
        except mysql.connector.Error as err:
            print(f"Error rejecting subscription: {err}")
        
        cursor.close()
        cnx.close()
    
    return redirect(url_for('admin_subscriptions'))

# --- RUN THE APP ---
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)