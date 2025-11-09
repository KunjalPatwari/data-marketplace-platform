# Node 2: Kafka Broker (Quant Model & Provisioning Service)


## Overview

This component is the central Kafka Broker (Node 2) for the "Internal Quant Analytics Marketplace." It serves as the high-throughput, reliable data pipeline connecting all other nodes.

This broker is configured to run in **KRaft mode**, eliminating the need for a separate Zookeeper cluster. Its primary responsibilities are:

  * Receiving data streams from the Producer (Node 1).
  * Serving data to the Consumer (Node 3).
  * Enforcing the **control plane** by disabling automatic topic creation, forcing all topics to be provisioned programmatically by the Producer's Admin API.

## Configuration

The core of this node's setup is the modification of the Kafka configuration file located at `/opt/kafka/config/kraft/server.properties`.

The following properties were modified or added:

  * **`log.dirs`**: This is a critical setting for persistence. It points to a permanent directory (`/opt/kafka/kraft-data`) instead of the default `/tmp/` folder. This ensures that all topics and data **survive a server reboot**.
  * **`advertised.listeners`**: This is the most important setting for a multi-node setup. It tells the broker to announce its public ZeroTier IP to all clients (Producers/Consumers).
  * **`listeners`**: This binds the broker to all available network interfaces (`0.0.0.0`), allowing it to accept connections from the ZeroTier adapter.
  * **`auto.create.topics.enable`**: This is the key to our project's "control plane." By setting this to `false`, we prevent any producer from creating a topic just by sending a message.

<!-- end list -->

```properties
# Permanent storage directory
log.dirs=/opt/kafka/kraft-data

# Public IP for clients
advertised.listeners=PLAINTEXT://<YOUR_ZEROTIER_IP_HERE>:9092,CONTROLLER://<YOUR_ZEROTIER_IP_HERE>:9093

# Listen on all network interfaces
listeners=PLAINTEXT://:9092,CONTROLLER://:9093

# Enforce control plane
auto.create.topics.enable=false
```

*(Note: The IP address in `advertised.listeners` must be changed to match the broker's current ZeroTier IP.)*

## Setup & Execution

### One-Time Setup

These commands must be run once to create the permanent storage directory and format it for KRaft mode.

```bash
# 1. Go to the Kafka directory
cd /opt/kafka

# 2. Create the permanent data folder
sudo mkdir /opt/kafka/kraft-data

# 3. Generate a unique Cluster ID
KAFKA_CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)

# 4. Format the new permanent storage directory
sudo bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
```

### Running the Service

This single command starts the broker. It must be left running in a terminal.

```bash
sudo bin/kafka-server-start.sh config/kraft/server.properties
```

-----