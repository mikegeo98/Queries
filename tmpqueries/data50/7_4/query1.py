from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType
from datetime import date, time, datetime

#spark = SparkSession.builder.appName("Query1_DFAPI").config("spark.yarn.am.memory","40960").config("spark.executor.memory","4g").config("spark.executor.pyspark.memory","2g").config("spark.executor.instances", "6").master("yarn").getOrCreate()
#spark.conf.set("spark.sql.join.preferSortMergeJoin", False)

spark = SparkSession.builder.appName("Query1_DFAPI").config("spark.executor.instances","7").config("spark.executor.cores","4").master("yarn").getOrCreate()
#spark = SparkSession.builder.appName("Query1_DFAPI").master("yarn").getOrCreate()
#spark = SparkSession.builder.appName("Query1_DFAPI").getOrCreate()
spark.conf.set("spark.sql.join.preferSortMergeJoin", False)

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("/user/diplomma/data/data50/lineitem.tbl")

lineitem.registerTempTable("lineitem")
queryStartTime = datetime.now()
sqlString= "select \
        l_returnflag, \
        l_linestatus, \
        sum(l_quantity) as sum_qty, \
        sum(l_extendedprice) as sum_base_price, \
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, \
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, \
        avg(l_quantity) as avg_qty, \
        avg(l_extendedprice) as avg_price, \
        avg(l_discount) as avg_disc, \
        count(*) as count_order \
from \
        lineitem \
where \
        l_shipdate <= date '1998-12-01' - interval '90' day \
group by \
        l_returnflag, \
        l_linestatus \
order by \
        l_returnflag, \
        l_linestatus; \
"

res = spark.sql(sqlString)
queryStopTime = datetime.now()
runTime = queryStopTime-queryStartTime 
res.show()
spark.sql(sqlString).explain()

print("Runtime: ",runTime)

tmp = lineitem.rdd.getNumPartitions()
print("Lineitem",tmp)

