import time
import random
from pymongo import MongoClient
from faker_persona_mx import PersonaGenerator
import logging

# Silenciar los logs de la librería para que no ensucien la consola
logging.getLogger("faker_persona_mx").setLevel(logging.CRITICAL)

# Conexión a tu réplica set
MONGO_URI = "mongodb://admin:testing@192.168.7.218:46000/?authSource=admin&replicaSet=rs0"
client = MongoClient(MONGO_URI)
db = client["SyntheticDB"]
users_col = db["Users"]

class PersonaBuffer:
    """Maneja la generación en lotes para no sobrecargar la memoria ni repetir datos."""
    def __init__(self, batch_size=100):
        self.batch_size = batch_size
        self.buffer = []

    def get_next_persona(self):
        if not self.buffer:
            # Si el buffer está vacío, generamos una nueva semilla y un nuevo lote
            semilla = random.randint(1, 4294967295)
            generator = PersonaGenerator(seed=semilla)
            # generate_batch ya descarta los erróneos automáticamente
            self.buffer = generator.generate_batch(self.batch_size) 
        
        # Sacamos y devolvemos el último elemento de la lista
        return self.buffer.pop()

def get_random_user():
    """Devuelve un usuario verdaderamente aleatorio de la colección usando $sample"""
    cursor = users_col.aggregate([{"$sample": {"size": 1}}])
    user_list = list(cursor)
    return user_list[0] if user_list else None

def live_traffic_simulator():
    print("Iniciando simulación de tráfico en vivo... Presiona Ctrl+C para detener.")
    
    # Inicializamos nuestro generador con buffer
    persona_buffer = PersonaBuffer(batch_size=50) 
    
    while True:
        action = random.choices(
            population=["CREATE", "UPDATE", "DELETE"],
            weights=[0.6, 0.3, 0.1], # 60% crear, 30% actualizar, 10% borrar
            k=1
        )[0]
        
        if action == "CREATE":
            p = persona_buffer.get_next_persona()
            new_user = {
                "nombre": p.nombre,
                "apellido_paterno": p.apellido_paterno,
                "apellido_materno": p.apellido_materno,
                "curp": p.curp,
                "rfc": p.rfc,
                "email": p.email,
                "telefono": p.telefono,
                "edad": random.randint(18, 90),
                "estatus": "activo"
            }
            users_col.insert_one(new_user)
            print(f"[+] Creado: {new_user['nombre']} {new_user['apellido_paterno']} ({new_user['email']})")

        elif action == "UPDATE":
            random_user = get_random_user()
            if random_user:
                new_age = random.randint(18, 90)
                users_col.update_one(
                    {"_id": random_user["_id"]}, 
                    {"$set": {"edad": new_age, "estatus": "actualizado"}}
                )
                print(f"[*] Actualizado: {random_user['nombre']} ahora tiene {new_age} años")

        elif action == "DELETE":
            random_user = get_random_user()
            if random_user:
                users_col.delete_one({"_id": random_user["_id"]})
                print(f"[-] Borrado: {random_user['nombre']} {random_user['apellido_paterno']}")

        time.sleep(1) # Espera 1 segundo entre operaciones

if __name__ == "__main__":
    live_traffic_simulator()