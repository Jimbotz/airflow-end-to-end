import random
from pymongo import MongoClient
from faker_persona_mx import PersonaGenerator

# Connection using the IP and Port that worked for you
MONGO_URI = "mongodb://admin:testing@192.168.7.218:46000/?authSource=admin&replicaSet=rs0"

client = MongoClient(MONGO_URI)
db = client["SyntheticDB"]
users_col = db["Users"]

def generate_bulk_mexican_users(n=1000):
    # 1. Initialize generator once
    semilla = 42
    generator = PersonaGenerator(seed=semilla)
    
    print(f"Generating {n} unique Mexican personas...")
    
    # 2. Generar TODAS las personas de una sola vez
    personas_generadas = generator.generate_batch(n)
    
    users = []
    # 3. Iterar sobre la lista de personas únicas
    for i, persona in enumerate(personas_generadas):
        user_doc = {
            "nombre": persona.nombre,
            "apellido_paterno": persona.apellido_paterno,
            "apellido_materno": persona.apellido_materno,
            "curp": persona.curp,
            "rfc": persona.rfc,
            "email": persona.email,
            "telefono": persona.telefono,
            "edad": random.randint(18, 90), 
            "metadata": {"batch": 1, "index": i}
        }
        users.append(user_doc)
    
    # 4. Bulk insert for efficiency
    if users:
        result = users_col.insert_many(users)
        print(f"Done! {len(result.inserted_ids)} unique users inserted.")

if __name__ == "__main__":
    # Wipe the old 'samey' data first if you want
    # users_col.delete_many({}) 
    generate_bulk_mexican_users(1000)