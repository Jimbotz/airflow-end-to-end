mongoPg/

├── docker-compose.yml          # Tu orquestador de contenedores

├── .env                        # Tus variables de entorno (contraseñas, puertos)

├── requirements.txt            # (Opcional) Lista de librerías pip instaladas
│

├── security/                   
│   └── mongodb.key
│

├── scripts/                    
│   └── init-rs.sh
│

├── 1_simulator/                # El sistema Faker
│   └── live_traffic_simulator.py
│

├── 2_cdc_pipeline/             # Tus conectores y puente Kafka
│   ├── initial_load.py
│   ├── mongo_to_kafka_producer.py
│   └── kafka_to_postgres_consumer.py
│

└── 3 dbt_analytics/            # <-- (Esta carpeta la creará dbt init automáticamente)

puerto de mongo: 46000
puerto de kafka: 46500
puerto de postgres 47000

user: admin
pass: testing
dbname: datawarehouse
schema: analytics