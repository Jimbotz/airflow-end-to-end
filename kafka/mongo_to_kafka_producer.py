from pymongo import MongoClient
from kafka import KafkaProducer
from bson.json_util import dumps
import sys

# Configuración de conexiones
MONGO_URI = "mongodb://admin:testing@localhost:46000/?authSource=admin&replicaSet=rs0"
KAFKA_BROKER = "localhost:46500"
KAFKA_TOPIC = "mongodb.syntheticdb.users.events"

print("Iniciando conexión a Kafka...")
try:
    # value_serializer se encarga de convertir el diccionario de Python a JSON (formato BSON) 
    # y luego a bytes, que es el único idioma que Kafka entiende.
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )
    print("✓ Conectado a Kafka exitosamente.")
except Exception as e:
    print(f"Error conectando a Kafka: {e}")
    sys.exit(1)

# Conexión a MongoDB
client = MongoClient(MONGO_URI)
db = client["SyntheticDB"]
users_col = db["Users"]

def start_cdc_producer():
    print(f"--- 🚀 Productor CDC Activo ---")
    print(f"Escuchando MongoDB y enviando eventos al tópico: {KAFKA_TOPIC}")

    pipeline = [{
        "$match": {
            "operationType": {"$in": ["insert", "update", "delete"]}
        }
    }]

    try:
        with users_col.watch(pipeline, full_document='updateLookup') as stream:
            for change in stream:
                op_type = change["operationType"]
                doc_id = change["documentKey"]["_id"]
                
                # 1. Enviar el evento COMPLETO a Kafka
                # Kafka no necesita saber qué significa, solo lo transporta
                producer.send(KAFKA_TOPIC, value=change)
                
                # 2. Imprimir en consola para que sepas qué está pasando
                if op_type == "insert":
                    nombre = change.get("fullDocument", {}).get("nombre", "Desconocido")
                    print(f"[+] INSERT -> Kafka: Se creó a {nombre} (ID: {doc_id})")
                elif op_type == "update":
                    print(f"[*] UPDATE -> Kafka: Cambios en ID {doc_id}")
                elif op_type == "delete":
                    print(f"[-] DELETE -> Kafka: Eliminado ID {doc_id}")
                
                # Forzar el envío inmediato del mensaje a Kafka
                producer.flush()

    except KeyboardInterrupt:
        print("\nProductor detenido por el usuario.")
    except Exception as e:
        print(f"Error en el stream: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    start_cdc_producer()