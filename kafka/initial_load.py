import psycopg2
from pymongo import MongoClient

# Conexiones
MONGO_URI = "mongodb://admin:testing@localhost:46000/?authSource=admin&replicaSet=rs0"
PG_URI = "postgresql://admin:testing@localhost:47000/datawarehouse"

def run_initial_load():
    print("--- 🚚 Iniciando Carga Histórica (Mongo -> Postgres) ---")
    
    # Conectar a Mongo
    client = MongoClient(MONGO_URI)
    users_col = client["SyntheticDB"]["Users"]
    
    # Conectar a Postgres
    pg_conn = psycopg2.connect(PG_URI)
    pg_cursor = pg_conn.cursor()
    
    # Extraer todos los documentos de Mongo
    historicos = list(users_col.find({}))
    print(f"Se encontraron {len(historicos)} registros en MongoDB.")
    
    insertados = 0
    for doc in historicos:
        mongo_id = str(doc["_id"])
        
        # A los usuarios viejos que no tenían estatus (porque los creaste antes del simulador), 
        # les asignamos el estatus "historico".
        estatus_inicial = doc.get("estatus", "historico")
        
        try:
            # ON CONFLICT DO NOTHING evita que dupliquemos si corremos el script dos veces
            pg_cursor.execute("""
                INSERT INTO raw_users (mongo_id, nombre, apellido_paterno, apellido_materno, curp, rfc, email, telefono, edad, estatus)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (mongo_id) DO NOTHING;
            """, (
                mongo_id, doc.get("nombre"), doc.get("apellido_paterno"), 
                doc.get("apellido_materno"), doc.get("curp"), doc.get("rfc"), 
                doc.get("email"), doc.get("telefono"), doc.get("edad"), estatus_inicial
            ))
            insertados += pg_cursor.rowcount
        except Exception as e:
            print(f"Error insertando {mongo_id}: {e}")
            pg_conn.rollback() # Prevenir que la transacción se congele si hay un error
            continue
            
    # Confirmar cambios en Postgres
    pg_conn.commit()
    print(f"✓ Migración completada. {insertados} registros históricos insertados en Postgres.")
    
    pg_cursor.close()
    pg_conn.close()

if __name__ == "__main__":
    run_initial_load()