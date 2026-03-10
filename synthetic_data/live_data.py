import time
import random
from pymongo import MongoClient
from faker import Faker

MONGO_URI = "mongodb://admin:testing@localhost:46000/?authSource=admin&replicaSet=rs0"
client = MongoClient(MONGO_URI)
db = client["SyntheticDB"]
users_col = db["Users"]
fake = Faker()

def live_traffic_simulator():
    print("Starting data chaos... Press Ctrl+C to stop.")
    while True:
        action = random.choice(["UPDATE", "CREATE", "DELETE"])
        
        if action == "CREATE":
            new_user = {"first_name": fake.first_name(), "last_name": fake.last_name(), "age": fake.random_int(min=18, max=80)}
            users_col.insert_one(new_user)
            print(f"[+] Created: {new_user['first_name']}")

        elif action == "UPDATE":
            # Find a random user to update
            random_user = users_col.find_one()
            if random_user:
                new_age = fake.random_int(min=18, max=80)
                users_col.update_one({"_id": random_user["_id"]}, {"$set": {"age": new_age}})
                print(f"[*] Updated: {random_user['first_name']} is now {new_age}")

        elif action == "DELETE":
            # Delete one random user
            random_user = users_col.find_one()
            if random_user:
                users_col.delete_one({"_id": random_user["_id"]})
                print(f"[-] Deleted: {random_user['first_name']}")

        time.sleep(1) # Wait 1 second

if __name__ == "__main__":
    live_traffic_simulator()