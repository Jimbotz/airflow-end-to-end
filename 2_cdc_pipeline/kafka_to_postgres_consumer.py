import json
import psycopg2
from kafka import KafkaConsumer

# Configuración
KAFKA_BROKER = "localhost:46500"
KAFKA_TOPIC = "mongodb.syntheticdb.users.events"
PG_URI = "postgresql://admin:testing@localhost:47000/datawarehouse"

def setup_postgres():
    """Crea la tabla cruda (raw) en Postgres si no existe"""
    conn = psycopg2.connect(PG_URI)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_users (
            mongo_id VARCHAR(50) PRIMARY KEY,
            nombre VARCHAR(100),
            apellido_paterno VARCHAR(100),
            apellido_materno VARCHAR(100),
            curp VARCHAR(20),
            rfc VARCHAR(15),
            email VARCHAR(150),
            telefono VARCHAR(20),
            edad INTEGER,
            estatus VARCHAR(50)
        );
    """)
    conn.commit()
    return conn, cursor

def start_consumer():
    print("--- 📥 Consumidor Kafka -> Postgres Activo ---")
    
    # Preparamos la base de datos
    pg_conn, pg_cursor = setup_postgres()
    print("✓ Tabla 'raw_users' lista en PostgreSQL.")

    # Conectamos a Kafka
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest', # Leer desde el momento en que se conecta
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print(f"Escuchando eventos en el tópico: {KAFKA_TOPIC}...\n")

    try:
        for message in consumer:
            evento = message.value
            op_type = evento.get("operationType")
            
            # Extraemos el ID de Mongo (Viene como {"$oid": "string..."} por el bson dump)
            mongo_id = evento["documentKey"]["_id"]["$oid"]

            if op_type == "insert":
                doc = evento.get("fullDocument", {})
                pg_cursor.execute("""
                    INSERT INTO raw_users (mongo_id, nombre, apellido_paterno, apellido_materno, curp, rfc, email, telefono, edad, estatus)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (mongo_id) DO NOTHING;
                """, (
                    mongo_id, doc.get("nombre"), doc.get("apellido_paterno"), 
                    doc.get("apellido_materno"), doc.get("curp"), doc.get("rfc"), 
                    doc.get("email"), doc.get("telefono"), doc.get("edad"), doc.get("estatus")
                ))
                print(f"[+] INSERT replicado en Postgres -> ID: {mongo_id}")

            elif op_type == "update":
                # Extraemos los campos que cambiaron
                cambios = evento["updateDescription"]["updatedFields"]
                
                # Construimos un query dinámico para actualizar solo lo que cambió
                set_clause = ", ".join([f"{k} = %s" for k in cambios.keys()])
                valores = list(cambios.values())
                valores.append(mongo_id) # Para el WHERE
                
                query = f"UPDATE raw_users SET {set_clause} WHERE mongo_id = %s"
                pg_cursor.execute(query, valores)
                print(f"[*] UPDATE replicado en Postgres -> ID: {mongo_id} | Cambios: {list(cambios.keys())}")

            elif op_type == "delete":
                pg_cursor.execute("DELETE FROM raw_users WHERE mongo_id = %s", (mongo_id,))
                print(f"[-] DELETE replicado en Postgres -> ID: {mongo_id}")

            # Guardamos los cambios en Postgres
            pg_conn.commit()

    except KeyboardInterrupt:
        print("\nConsumidor detenido.")
    except Exception as e:
        print(f"Error procesando mensaje: {e}")
    finally:
        pg_cursor.close()
        pg_conn.close()
        consumer.close()

if __name__ == "__main__":
    start_consumer()