from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
df = spark.createDataFrame([(1, "ok")], ["id", "status"])
df.show()
spark.stop()