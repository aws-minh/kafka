from confluent_kafka import Producer
import time
import random
import json
from datetime import datetime, timedelta

def read_config():
    # Reads the client configuration from client.properties and returns it as a key-value map
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                config[parameter] = value.strip()
    return config

def generate_sample_data():
    categories = ["Electronics", "Clothing", "Books", "Home"]
    products = {
        "Electronics": ["Smartphone", "Laptop", "Headphones", "Camera"],
        "Clothing": ["Shirt", "Jeans", "Jacket", "Shoes"],
        "Books": ["Fiction", "Non-Fiction", "Comics", "Textbook"],
        "Home": ["Furniture", "Decor", "Kitchenware", "Bedding"]
    }
    
    data = []
    base_date = datetime.now()

    for _ in range(100):
        customer_id = random.randint(1000, 9999)
        day = base_date.strftime('%Y-%m-%d')
        amount = round(random.uniform(5.0, 500.0), 2)
        quantity = random.randint(1, 10)
        category = random.choice(categories)
        product = random.choice(products[category])
        sales_item = f"{category}_{product}"

        record = {
            "customer_id": customer_id,
            "day": day,
            "amount": amount,
            "quantity": quantity,
            "sales_item": sales_item,
            "product": product,
            "category": category
        }
        
        data.append(record)
    
    return data

def main():
    config = read_config()
    topic = "sales"
    
    # Creates a new producer instance
    producer = Producer(config)
    
    # Continuously generate and produce sample messages
    while True:
        sample_data = generate_sample_data()
        for record in sample_data:
            key = str(record["customer_id"])
            value = json.dumps(record)
            producer.produce(topic, key=key, value=value)
            print(f"Produced message to topic {topic}: key = {key:12} value = {value}")
        # Send any outstanding or buffered messages to the Kafka broker
        producer.flush()
        # Pause for 1 second before generating the next batch of data
        time.sleep(1)

if __name__ == "__main__":
    main()
