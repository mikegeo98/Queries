from os.path import abspath
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType
from datetime import date,time,datetime



warehouse_location = abspath('spark-warehouse')

spark = SparkSession.builder.appName("Query6_DFAPI").master("yarn").config("spark.sql.warehouse.dir", warehouse_location).enableHiveSupport().getOrCreate()


#spark = SparkSession.builder.appName("Query1_DFAPI").getOrCreate()

#fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema/lineitem.csv").schema
#lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/lineitem.tbl")

#lineitem.registerTempTable("lineitem")


sqlString="select \
        sum(l_extendedprice * l_discount) as revenue \
from \
        lineitem \
where \
        l_shipdate >= date '1994-01-01' \
        and l_shipdate < date '1994-01-01' + interval '1' year \
        and l_discount between .06 - 0.01 and .06 + 0.01 \
        and l_quantity < 24; \
"
queryStartTime = datetime.now()
res = spark.sql(sqlString)
queryStopTime = datetime.now()
runTime = queryStopTime-queryStartTime 
res.show()

print("Runtime: ",runTime)

