from confluent_kafka import Producer
import random
import json
from datetime import datetime, timedelta
import time

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

def generate_customer_data(num_customers):
    countries = ["USA", "UK", "Canada", "Australia", "Germany", "France", "India", "Japan"]
    carriers = ["Verizon", "AT&T", "T-Mobile", "Sprint", "Vodafone", "Orange", "Telefonica", "NTT Docomo"]
    genders = ["Male", "Female", "Other"]
    
    customers = []

    for _ in range(num_customers):
        customer_id = random.randint(1000, 9999)
        customer_name = f"Customer_{customer_id}"
        email = f"customer{customer_id}@example.com"
        phone_number = f"+1{random.randint(200, 999)}{random.randint(100, 999)}{random.randint(1000, 9999)}"
        phone_carrier = random.choice(carriers)
        country = random.choice(countries)
        address = f"{random.randint(100, 999)} {random.choice(['Main', 'Oak', 'Elm', 'Maple'])} St, {random.choice(['Apt', 'Suite'])}. {random.randint(100, 999)}, {random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego'])}, {country}"
        age = random.randint(18, 90)
        gender = random.choice(genders)
        
        customer = {
            "customer_id": customer_id,
            "customer_name": customer_name,
            "email": email,
            "phone_number": phone_number,
            "phone_carrier": phone_carrier,
            "country": country,
            "address": address,
            "age": age,
            "gender": gender
        }
        
        customers.append(customer)
    
    return customers

def generate_sample_data(num_samples):
    categories = ["Electronics", "Clothing", "Books", "Home", "Sports", "Beauty", "Toys", "Food"]
    products = {
        "Electronics": ["Smartphone", "Laptop", "Headphones", "Camera", "Smartwatch", "Tablet", "TV", "Gaming Console"],
        "Clothing": ["Shirt", "Jeans", "Jacket", "Shoes", "Dress", "Sweater", "Skirt", "T-shirt"],
        "Books": ["Fiction", "Non-Fiction", "Comics", "Textbook", "Biography", "Self-Help", "Mystery", "Cookbook"],
        "Home": ["Furniture", "Decor", "Kitchenware", "Bedding", "Appliances", "Lighting", "Storage", "Rugs"],
        "Sports": ["Soccer Ball", "Basketball", "Yoga Mat", "Dumbbells", "Tennis Racket", "Running Shoes", "Gym Bag", "Fitness Tracker"],
        "Beauty": ["Shampoo", "Conditioner", "Body Lotion", "Facial Cleanser", "Makeup", "Perfume", "Hairbrush", "Razor"],
        "Toys": ["Action Figure", "Doll", "Building Blocks", "Board Game", "Puzzle", "Remote Control Car", "Stuffed Animal", "Play Kitchen"],
        "Food": ["Snacks", "Cereal", "Canned Goods", "Beverages", "Frozen Food", "Fresh Produce", "Dairy", "Condiments"]
    }
    
    data = []
    base_date = datetime.now()

    for _ in range(num_samples):
        customer_id = random.randint(1000, 9999)
        day = (base_date - timedelta(days=_)).strftime('%Y-%m-%d')
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
    sales_topic = "sales"
    customer_topic = "customer"
    num_samples_per_second = 100
    delay_between_batches = 1 / num_samples_per_second
    
    # Creates a new producer instance for sales data
    sales_producer = Producer(config)
    # Creates a new producer instance for customer data
    customer_producer = Producer(config)
    
    while True:
        start_time = time.time()
        
        # Generate customer data
        customers = generate_customer_data(num_samples_per_second)
        
        # Produce customer dimension data
        for customer in customers:
            key = str(customer["customer_id"])
            value = json.dumps(customer)
            customer_producer.produce(customer_topic, key=key, value=value)
            print(f"Produced customer data to topic {customer_topic}: key = {key}, value = {value}")
        
        # Send any outstanding or buffered customer messages to the Kafka broker
        customer_producer.flush()
        
        # Generate and produce sample sales data
        sample_data = generate_sample_data(num_samples_per_second)
        for record in sample_data:
            key = str(record["customer_id"])
            value = json.dumps(record)
            sales_producer.produce(sales_topic, key=key, value=value)
            print(f"Produced sales data to topic {sales_topic}: key = {key}, value = {value}")
        
        # Send any outstanding or buffered sales messages to the Kafka broker
        sales_producer.flush()
        
        # Calculate elapsed time and introduce delay if needed
        elapsed_time = time.time() - start_time
        if elapsed_time < delay_between_batches:
            time.sleep(delay_between_batches - elapsed_time)

if __name__ == "__main__":
    main()
