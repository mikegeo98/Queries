from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Converter").getOrCreate()
spark = SparkSession.builder.appName("Query3_DFAPI").config("spark.yarn.am.memory","40960").config("spark.executor.memory","4g").config("spark.executor.pyspark.memory","2g").config("spark.executor.instances", "6").master("yarn").getOrCreate()
#sc = spark.sparkContext
dfw = spark.read.format('csv').options(header='false', inferSchema='true').load("/user/diplomma/data/orders.csv")
#dfw = spark.read.csv("hdfs://master:9000/InitialFiles/movies.csv")
dfw.write.parquet("/user/diplomma/data/orders.parquet")
