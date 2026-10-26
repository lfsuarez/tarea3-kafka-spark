import csv
import json
import time
from kafka import KafkaProducer

# Inicializar el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'sensor_data'
csv_file_path = 'creditcard.csv'

print("Iniciando envío de transacciones a Kafka...")

with open(csv_file_path, mode='r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        # Extraer los datos y convertirlos a sus tipos correspondientes
        data = {
            'Time': float(row['Time']),
            'Amount': float(row['Amount']),
            'Class': int(row['Class'])
        }
        
        # Incluir variables anónimas V1 a V28
        for i in range(1, 29):
            col_name = f'V{i}'
            if col_name in row:
                data[col_name] = float(row[col_name])

        # Enviar el mensaje al topic
        producer.send(topic_name, value=data)
        print(f"Transacción enviada: {data}")
        
        # Pausa de 0.05 segundos para simular flujo en vivo
        time.sleep(0.05)

producer.flush()
producer.close()
