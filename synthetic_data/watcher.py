from pymongo import MongoClient
import json

# Connection to your secure Manjaro-hosted container
MONGO_URI = "mongodb://admin:testing@localhost:46000/?authSource=admin&replicaSet=rs0"

client = MongoClient(MONGO_URI)
db = client["SyntheticDB"]
users_col = db["Users"]

def start_watcher():
    print("--- 👁️ Real-Time All-Events Watcher Active ---")
    print("Listening for inserts, updates, and deletes in SyntheticDB.Users...")

    # Ampliamos el pipeline para escuchar los tres tipos de operaciones principales
    pipeline = [{
        "$match": {
            "operationType": {"$in": ["insert", "update", "delete"]}
        }
    }]

    try:
        # full_document='updateLookup' le dice a Mongo que cuando haya un UPDATE, 
        # además de decirte qué cambió, te mande la versión final del documento.
        with users_col.watch(pipeline, full_document='updateLookup') as stream:
            for change in stream:
                op_type = change["operationType"]
                doc_id = change["documentKey"]["_id"]
                
                if op_type == "insert":
                    # Extraemos el documento recién creado
                    full_doc = change.get("fullDocument", {})
                    nombre = full_doc.get("nombre", "Desconocido")
                    print(f"[+] INSERT: Se creó a {nombre} (ID: {doc_id})")
                
                elif op_type == "update":
                    # Extraemos exactamente qué campos sufrieron modificaciones
                    cambios = change["updateDescription"]["updatedFields"]
                    print(f"[*] UPDATE: El documento {doc_id} cambió. Nuevos valores: {cambios}")
                
                elif op_type == "delete":
                    # En los borrados, el documento ya no existe, solo nos queda el ID
                    print(f"[-] DELETE: Documento eliminado definitivamente -> ID: {doc_id}")
                
    except KeyboardInterrupt:
        print("\nWatcher stopped by user.")
    except Exception as e:
        print(f"Error in stream: {e}")

if __name__ == "__main__":
    start_watcher()