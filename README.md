# Detección de Fraude en Tarjetas de Crédito - Apache Spark & Kafka

**Autor:** Luis Fernando Suárez Molina  
**Universidad:** Universidad Nacional Abierta y a Distancia (UNAD)  
**Programa:** Ingeniería Industrial  
**Curso:** Procesamiento de Datos con Apache Spark (Tarea 3)

## Descripción del Proyecto
Este repositorio contiene la implementación de una arquitectura de Big Data orientada al procesamiento de eventos en tiempo real (Streaming). El objetivo es analizar el dataset `creditcard.csv` (Kaggle) con 284.807 transacciones financieras, el cual presenta un desbalance extremo (solo el 0.17% son fraudes), simulando un entorno de producción para la detección temprana de anomalías.

## Arquitectura y Tecnologías
*   **Apache ZooKeeper:** Coordinación y gestión del clúster.
*   **Apache Kafka:** Broker de mensajería (Topic: `sensor_data`).
*   **Apache Spark (PySpark):** Procesamiento en micro-lotes (Spark Streaming).
*   **Python 3:** Lenguaje base para los scripts.

## Archivos del Repositorio
1.  `kafka_producer.py`: Script que lee el dataset local y actúa como productor, publicando cada transacción en formato JSON hacia el topic de Kafka simulando un flujo en vivo.
2.  `spark_streaming_consumer.py`: Script consumidor que se suscribe al topic mediante el conector de Spark-Kafka. Procesa el flujo de datos por micro-lotes y calcula métricas clave (promedio de montos y conteo de eventos) agrupados por la clase de transacción (Fraude/Legítima).

## Instrucciones de Ejecución

**1. Levantar la infraestructura:**
```bash
sudo /opt/Kafka/bin/zookeeper-server-start.sh -daemon /opt/Kafka/config/zookeeper.properties
sudo /opt/Kafka/bin/kafka-server-start.sh -daemon /opt/Kafka/config/server.properties
