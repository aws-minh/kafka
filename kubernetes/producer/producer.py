from kafka import KafkaProducer
import json
import time
import random

# Kafka credentials
bootstrap_servers = "pkc-56d1g.eastus.azure.confluent.cloud:9092"
security_protocol = "SASL_SSL"
sasl_mechanism = "PLAIN"
sasl_plain_username = "2GQZHXZS45VQ7PJO"
sasl_plain_password = "ZCpmF1goGE4bQH7YaZnJnTzQQTzAyGKBr7aMciru8IozDwN6Z/OgnyXmjIQaA0L2"

def generate_sales_data(customer_id, day):
    amount = round(random.uniform(10, 1000), 2)
    quantity = random.randint(1, 10)
    # Sample sales items, products, and categories
    sales_items = ["item1", "item2", "item3"]
    products = ["product1", "product2", "product3"]
    categories = ["category1", "category2", "category3"]
    sales_item = random.choice(sales_items)
    product = random.choice(products)
    category = random.choice(categories)
    return {"customer_id": customer_id, "day": day, "amount": amount, "quantity": quantity, "sales_item": sales_item, "product": product, "category": category}

def main():
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                             security_protocol=security_protocol,
                             sasl_mechanism=sasl_mechanism,
                             sasl_plain_username=sasl_plain_username,
                             sasl_plain_password=sasl_plain_password,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    for day in range(1, 366):
        for customer_id in range(1, 101):
            sales_data = generate_sales_data(customer_id, day)
            producer.send('sale', value=sales_data)
            time.sleep(0.1)  # Adjust sleep time as needed

    producer.close()

if __name__ == "__main__":
    main()
