# Consumer Service (Node 3) – Dynamic Kafka Subscriber

This service represents the **Trading Bot / Data Subscriber** in the Data Marketplace Platform. 
It dynamically subscribes to Kafka topics based on permissions provided by the **Admin Service (Node 4)** through a REST API.

The consumer **does not require a restart** to pick up new topics. 
If a topic is approved and active, the consumer subscribes. 
If a topic is deactivated, the consumer unsubscribes automatically.


## Features
1. Connects to Kafka Broker (Node 2)
2. Fetches topic permissions from Flask API (Node 4)
3. Dynamically subscribes/unsubscribes from topics
4. Graceful recovery if Admin API or Kafka Broker temporarily goes down
5. Uses environment variables for all sensitive configuration
6. Prints received messages to console


## How to Run

1. Run the following command to clone the repository
`git clone https://github.com/KunjalPatwari/data-marketplace-platform.git`

2. Connect to a runtime and navigate to the node_3_consumer directory

3. Install requirements by running the following command on terminal or installing the mentioned dependencies in `requirements.txt`
`pip install -r requirements.txt`

4. Setup an environment file with required credentials

5. Run the following command on terminal
`python consumer.py`

## Author
@sudiksha-chindula[https://github.com/sudiksha-chindula]




