from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType

# Definir el esquema completo del mensaje JSON
schema_fields = [
    StructField("Time", DoubleType(), True),
    StructField("Amount", DoubleType(), True),
    StructField("Class", IntegerType(), True)
] + [StructField(f"V{i}", DoubleType(), True) for i in range(1, 29)]

schema = StructType(schema_fields)

# Iniciar la sesión de Spark
spark = SparkSession.builder \
    .appName("DeteccionFraudeStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Suscribirse al topic de Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "latest") \
    .load()

# Convertir el valor en texto JSON y parsearlo con el esquema
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")

# Calcular promedio de monto y conteo agrupado por Clase
aggregated_df = parsed_df.groupBy("Class") \
    .agg(
        avg("Amount").alias("avg(Amount)"),
        count("Time").alias("count(Time)")
    )

# Mostrar la tabla en consola por cada micro-lote
query = aggregated_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()