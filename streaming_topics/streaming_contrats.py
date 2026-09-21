# streaming_contrats.py
#
# Objectif : lire en continu les messages du topic Kafka "abassurance.contrats.v1"
# et les écrire dans HDFS (dossier /data/topics/contrats), au fur et à mesure
# qu'ils arrivent.
#
# Doc officielle Spark Structured Streaming :
# https://spark.apache.org/docs/latest/streaming/getting-started.html
#
# Doc officielle du connecteur Kafka pour Spark Structured Streaming :
# https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


# ------------------------------------------------------------------
# 1. Le "schéma" : on décrit à Spark la forme du JSON qu'il va recevoir
#    de Kafka. Doit correspondre exactement aux clés produites par
#    le tJavaFlex Talaxie.
#    Doc StructType/StructField :
#    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.StructType.html
# ------------------------------------------------------------------
schema_contrat = StructType([
    StructField("contrat_id", StringType()),
    StructField("client_id", StringType()),
    StructField("type_assurance", StringType()),
    StructField("code_produit", StringType()),
    StructField("date_debut", StringType()),
    StructField("date_fin", StringType()),
    StructField("prime_annuelle", DoubleType()),
    StructField("prime_mensuelle", DoubleType()),
    StructField("statut_contrat", StringType()),
    StructField("agence_id", StringType()),
    StructField("code_courtier", StringType()),
])


# ------------------------------------------------------------------
# 2. Le point d'entrée de toute application Spark : la SparkSession.
#    Doc SparkSession :
#    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html
# ------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("StreamingContrats") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")  # évite d'inonder la console de logs techniques


# ------------------------------------------------------------------
# 3. On branche Spark sur Kafka.
#    readStream = flux continu (contrairement à read = lecture ponctuelle).
#    Doc "Creating a Kafka Source Stream" (section de la doc Kafka Integration) :
#    https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#creating-a-kafka-source-stream
# ------------------------------------------------------------------
df_kafka_brut = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "abassurance.contrats.v1") \
    .option("startingOffsets", "earliest") \
    .load()


# ------------------------------------------------------------------
# 4. Ce que Spark reçoit de Kafka n'est pas directement lisible : le
#    contenu du message ("value") est en binaire brut. On le convertit
#    en texte, puis on le range dans les colonnes selon le schéma défini
#    à l'étape 1.
#    Doc from_json :
#    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html
# ------------------------------------------------------------------
df_texte = df_kafka_brut.selectExpr("CAST(value AS STRING) AS json_texte")
df_contrats = df_texte.select(
    from_json(col("json_texte"), schema_contrat).alias("donnees")
).select("donnees.*")


# ------------------------------------------------------------------
# 5. On écrit le résultat en continu dans HDFS, au format Parquet
#    (format compact et efficace pour le Big Data), dans le dossier
#    créé pour l'US4.1.
#    Doc "Starting Streaming Queries" :
#    https://spark.apache.org/docs/latest/streaming/getting-started.html#quick-example
# ------------------------------------------------------------------
requete = df_contrats.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/data/kafka/contrats") \
    .option("checkpointLocation", "hdfs://namenode:9000/data/checkpoints/contrats") \
    .trigger(processingTime="30 seconds") \
    .outputMode("append") \
    .start()

# awaitTermination() = le script reste actif en continu (comme une radio
# qu'on laisse allumée) tant qu'on ne l'arrête pas manuellement (Ctrl+C)
requete.awaitTermination()