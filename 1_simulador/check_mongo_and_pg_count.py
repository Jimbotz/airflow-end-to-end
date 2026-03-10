import psycopg2
from pymongo import MongoClient

# Cadenas de conexión exactas de tu entorno local
MONGO_URI = "mongodb://admin:testing@localhost:46000/?authSource=admin&replicaSet=rs0"
PG_URI = "postgresql://admin:testing@localhost:47000/datawarehouse"

def auditar_bases_de_datos():
    try:
        # 1. Conteo en MongoDB (Origen)
        client = MongoClient(MONGO_URI)
        db = client["SyntheticDB"]
        coleccion = db["Users"]
        mongo_total = coleccion.count_documents({})
        
        # 2. Conteo en PostgreSQL (Destino)
        pg_conn = psycopg2.connect(PG_URI)
        pg_cursor = pg_conn.cursor()
        
        # Contamos la tabla cruda (que es el reflejo exacto de Mongo)
        pg_cursor.execute("SELECT count(*) FROM raw_users;")
        pg_total = pg_cursor.fetchone()[0]
        
        pg_cursor.close()
        pg_conn.close()

        # 3. Imprimir el reporte
        print("\n📊 --- REPORTE DE SINCRONIZACIÓN EN TIEMPO REAL --- 📊")
        print(f"🟢 MongoDB (Origen):      {mongo_total} registros")
        print(f"🔵 PostgreSQL (Destino):  {pg_total} registros")
        print("-" * 50)
        
        diferencia = mongo_total - pg_total
        
        if diferencia == 0:
            print("✅ ¡Sincronización PERFECTA! No hay pérdida de datos en la tubería.")
        elif diferencia > 0:
            print(f"⚠️ Postgres está atrasado por {diferencia} registros. (¿El consumidor está apagado o Kafka está procesando?)")
        else:
            print(f"⚠️ Postgres tiene {abs(diferencia)} registros DE MÁS. (Posibles duplicados o eliminaciones no reflejadas).")
            
        print("\n")
            
    except Exception as e:
        print(f"❌ Error al conectar a las bases de datos: {e}")

if __name__ == "__main__":
    auditar_bases_de_datos()