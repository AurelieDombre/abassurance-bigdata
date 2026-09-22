
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestStockageHDFS") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

df = spark.createDataFrame([("test", 1)], ["nom", "valeur"])
df.write.mode("overwrite").parquet("hdfs://namenode:9000/data/raw/contrats/_test")

df_relu = spark.read.parquet("hdfs://namenode:9000/data/raw/contrats/_test")
df_relu.show()

spark.stop()