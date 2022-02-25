from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType
from datetime import date, time, datetime
import sys
disabled = sys.argv[1]
spark = SparkSession.builder.appName("Query3_DFAPI").getOrCreate()

if disabled == "Y":
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
elif disabled == 'N':
        pass
else:
        raise Exception ("This setting is not available.")

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("/user/diplomma/data/data/lineitem.tbl")

orderSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data//schema/orders.csv").schema
orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(orderSchema).option("delimiter", "|").csv("/user/diplomma/data/data/orders.tbl")

customerSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/customer.csv").schema
customer = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(customerSchema).option("delimiter", "|").csv("/user/diplomma/data/data/customer.tbl")

lineitem.registerTempTable("lineitem")
orders.registerTempTable("orders")
customer.registerTempTable("customer")

sqlString="select \
        l_orderkey, \
        sum(l_extendedprice * (1 - l_discount)) as revenue, \
        o_orderdate, \
        o_shippriority \
from \
        customer,\
        orders, \
        lineitem \
where \
        c_mktsegment = 'BUILDING' \
        and c_custkey = o_custkey \
        and l_orderkey = o_orderkey \
        and o_orderdate < date '1995-03-15' \
        and l_shipdate > date '1995-03-15' \
group by \
        l_orderkey, \
        o_orderdate, \
        o_shippriority \
order by \
        revenue desc, \
        o_orderdate; "

queryStartTime = datetime.now()
res = spark.sql(sqlString)
queryStopTime = datetime.now()
runTime = queryStopTime-queryStartTime 
res.show()

print("Runtime: ",runTime)

