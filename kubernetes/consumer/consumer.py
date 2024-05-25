from confluent_kafka import Consumer


def read_config():
    # Reads the client configuration from client.properties
    # and returns it as a key-value map
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                config[parameter] = value.strip()
    return config


def main():
    config = read_config()
    customer_topic = "customer"
    sales_topic = "sales"
  
    # Sets the consumer group ID and offset
    config["group.id"] = "python-group-1"
    config["auto.offset.reset"] = "earliest"

    # Creates a new consumer and subscribes to both topics
    consumer = Consumer(config)
    consumer.subscribe([customer_topic, sales_topic])
  
    try:
        while True:
            # Consumer polls the topics and prints any incoming messages
            msg = consumer.poll(1.0)
            if msg is not None and msg.error() is None:
                topic = msg.topic()
                key = msg.key().decode("utf-8")
                value = msg.value().decode("utf-8")
                print(f"Consumed message from topic {topic}: key = {key}, value = {value}")
    except KeyboardInterrupt:
        pass
    finally:
        # Closes the consumer connection
        consumer.close()


if __name__ == "__main__":
    main()
