import json
import random
import time
from datetime import datetime, timedelta
import os

def generate_transaction(transaction_id):
    locations = ["New York, USA", "San Francisco, USA", "London, UK", "Tokyo, Japan", "Sydney, Australia"]
    transaction_types = ["purchase", "withdrawal", "transfer"]
    
    transaction = {
        "transaction_id": str(transaction_id),
        "user_id": str(random.randint(100, 200)),
        "amount": round(random.uniform(10.0, 1000.0), 2),
        "timestamp": datetime.utcnow().isoformat() + 'Z',
        "location": random.choice(locations),
        "transaction_type": random.choice(transaction_types),
        "is_fraud": random.choice([True, False])
    }
    return transaction

def main():
    transaction_id = 1
    
    while True:
        transactions = []
        for _ in range(10):
            transaction = generate_transaction(transaction_id)
            transactions.append(transaction)
            transaction_id += 1
        
        # Output transactions to a JSON file
        output_directory = "output"
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        
        file_path = os.path.join(output_directory, "transaction.json")
        with open(file_path, "a") as outfile:
            json.dump(transactions, outfile)
            outfile.write('\n')  # Add newline between each batch of transactions
        
        # Sleep for an hour
        time.sleep(3600)

if __name__ == "__main__":
    main()
