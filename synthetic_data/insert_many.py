import os
from pymongo import MongoClient
from faker import Faker

# Use your Manjaro IP and the boss's port 46000
# Normally we'd use .env, but for a quick script:
MONGO_URI = "mongodb://admin:testing@localhost:46000/?authSource=admin&replicaSet=rs0"

client = MongoClient(MONGO_URI)
db = client["SyntheticDB"]
users_col = db["Users"]

fake = Faker()

def generate_bulk_users(n=1000):
    users = []
    for _ in range(n):
        user = {
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "age": fake.random_int(min=18, max=80),
            "email": fake.email(),
            "city": fake.city()
        }
        users.append(user)
    
    result = users_col.insert_many(users)
    print(f"Successfully inserted {len(result.inserted_ids)} users.")

if __name__ == "__main__":
    generate_bulk_users()