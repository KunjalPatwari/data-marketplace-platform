# Node 2: Kafka Broker

**Author:** Vishal
**Role:** Broker

## 1\. Component Overview

This component is the central Kafka Broker (Node 2) for the "Internal Quant Analytics Marketplace." It serves as the high-throughput, reliable data pipeline connecting all other nodes.

This broker is configured to run in **KRaft mode**, eliminating the need for a separate Zookeeper cluster. Its primary responsibilities are:

  * Receiving data streams from the Producer (Node 1).
  * Serving data to the Consumer (Node 3).
  * Enforcing the **control plane** by disabling automatic topic creation, forcing all topics to be provisioned programmatically by the Producer's Admin API.

## 2\. Key Configuration

The core of this node's setup is the modification of the Kafka configuration file located at `/opt/kafka/config/kraft/server.properties`.

The following properties were modified or added:

  * **`advertised.listeners`**: This is the most critical setting for a multi-node setup. It tells the broker to announce its public ZeroTier IP (e.g., `192.168.196.128`) to all clients. Without this, the Producer and Consumer nodes would be told to connect to `localhost` and fail.
  * **`listeners`**: This binds the broker to all available network interfaces (`0.0.0.0`), allowing it to accept connections from the ZeroTier adapter and not just from the local machine.
  * **`auto.create.topics.enable`**: This is the key to our project's "control plane." By setting this to `false`, we prevent any producer from creating a topic just by sending a message. All topics *must* be programmatically created via the KafkaAdminClient (handled by Node 1).

<!-- end list -->

```properties
advertised.listeners=PLAINTEXT://192.168.196.128:9092,CONTROLLER://192.168.196.128:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
auto.create.topics.enable=false
```

*(Note: The IP address in `advertised.listeners` must be changed to match the broker's current ZeroTier IP.)*

## 3\. Setup & Execution

### 3.1. One-Time Setup

These commands must be run once to format the broker's storage for KRaft mode.

```bash
# 1. Go to the Kafka directory
cd /opt/kafka

# 2. Generate a unique Cluster ID
KAFKA_CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)

# 3. Format the storage directory (uses sudo for /opt/ permissions)
sudo bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
```

### 3.2. Running the Service

This single command starts the broker. It must be left running in a terminal for the duration of the project's operation.

```bash
sudo bin/kafka-server-start.sh config/kraft/server.properties
```

-----