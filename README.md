# Detección de Fraude Financiero con Apache Spark & Kafka

**Autor:** Luis Suárez

**Programa:** Ingeniería Industrial

**Institución:** Universidad Nacional Abierta y a Distancia (UNAD)

**Curso:** Big Data

**Fase:** Tarea 3: Procesamiento de Datos con Apache Spark

---
## 📋 Descripción del Proyecto
Este proyecto implementa un entorno de procesamiento distribuido en tiempo real para la detección de fraudes en transacciones con tarjetas de crédito. Se utiliza el dataset `creditcard.csv` (Kaggle) con 284.807 registros altamente desbalanceados.

## 🛠️ Arquitectura y Componentes
* **Apache ZooKeeper:** Coordinador de servicios del clúster.
* **Apache Kafka:** Broker de eventos distribuido (Topic: `sensor_data`).
* **Apache Spark (PySpark):** Procesamiento de micro-lotes en tiempo real mediante `SparkSession` y `readStream`.
* **Python 3:** Entorno de scripting para producción y consumo.

## 📁 Archivos Incluidos
* `kafka_producer.py`: Lee el archivo local de transacciones y las transmite progresivamente en formato JSON hacia Kafka.
* `spark_streaming_consumer.py`: Se suscribe al topic de Kafka, procesa los micro-lotes y calcula estadísticas agrupadas (promedio de monto y volumen por clase).
* `README.md`: Documentación técnica del repositorio.

## 🚀 Guía de Ejecución

1. **Iniciar la infraestructura:**
   ```bash
   sudo /opt/Kafka/bin/zookeeper-server-start.sh -daemon /opt/Kafka/config/zookeeper.properties
   sudo /opt/Kafka/bin/kafka-server-start.sh -daemon /opt/Kafka/config/server.properties
   /opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor_data
   ```

2. **Inspeccionar la estructura del dataset:**
   ```bash
   head -n 1 creditcard.csv
   ```

3. **Ejecutar el productor de eventos:**
   ```bash
   python3 kafka_producer.py
   ```

4. **Ejecutar el consumidor de Spark Streaming:**
   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
   ```
   ![Ejecución en tiempo real de Spark y Kafka](./arquitectura.png)

    ![Ejecución en tiempo real de Spark y Kafka](./oo.png)
