from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('VoirDonnees').config('spark.hadoop.fs.defaultFS', 'hdfs://namenode:9000').getOrCreate()
df = spark.read.parquet('hdfs://namenode:9000/data/kafka/sinistres')
df.show(20, truncate=False)
print('Nombre total de lignes :', df.count())
spark.stop()