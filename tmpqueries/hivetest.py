from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType
from datetime import date, time, datetime

spark = SparkSession.builder.appName("Query1_DFAPI").master("yarn").getOrCreate()
#spark = SparkSession.builder.appName("Query1_DFAPI").getOrCreate()

#fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/lineitem.csv").schema
#lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("/user/diplomma/data/data10/lineitem.tbl")

#regionSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/region.csv").schema
#region = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(regionSchema).option("delimiter", "|").csv("/user/diplomma/data/data10/region.tbl")

#orderSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/orders.csv").schema
#orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(orderSchema).option("delimiter", "|").csv("/user/diplomma/data/data10/orders.tbl")

#customerSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/customer.csv").schema
#customer = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(customerSchema).option("delimiter", "|").csv("/user/diplomma/data/data10/customer.tbl")

#customer.registerTempTable("customer")
#region.registerTempTable("region")
#lineitem.registerTempTable("lineitem")
#orders.registerTempTable("orders")
#queryStartTime = datetime.now()
sqlString2= "select \
        o_orderkey, o_orderdate, o_shippriority \
from \
        orders  \
where \
	o_orderdate < date '1995-03-15' \
"
sqlString3  = "select \
        l_orderkey,l_extendedprice \
from \
        lineitem  \
where \
        l_shipdate > date '1995-03-15' \
"
sqlString = "select \
        l_orderkey, \
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
        and l_shipdate > date '1995-03-15'; \
"


queryStartTime = datetime.now()
q1 = datetime.now()
spark.sql(sqlString) #.explain()
q1f = datetime.now()
q2 = datetime.now()
spark.sql(sqlString2)
q2f = datetime.now()
q3 = datetime.now()
spark.sql(sqlString3)
q3f = datetime.now()
q4 = datetime.now()
spark.sql(sqlString)
q4f = datetime.now()
q5 = datetime.now()
spark.sql(sqlString)
q5f = datetime.now()
q6 = datetime.now()
spark.sql(sqlString)
q6f = datetime.now()
q7 = datetime.now()
spark.sql(sqlString)
q7f = datetime.now()
q8 = datetime.now()
spark.sql(sqlString)
q8f = datetime.now()
q9 = datetime.now()
spark.sql(sqlString)
q9f = datetime.now()
q10 = datetime.now()
res = spark.sql(sqlString)
#res.show()
q10f = datetime.now()
queryStopTime = datetime.now()
#res.show()
run1 = q1f - q1
run2 = q2f - q2
run3 = q3f - q3
run4 = q4f - q4
run5 = q5f - q5
run6 = q6f - q6
run7 = q7f - q7
run8 = q8f - q8
run9 = q9f - q9
run10 = q10f - q10

runTime = queryStopTime-queryStartTime
#res.show()
print("Runtime: ",run1)
print("Runtime: ",run2)
print("Runtime: ",run3)
print("Runtime: ",run4)
print("Runtime: ",run5)
print("Runtime: ",run6)
print("Runtime: ",run7)
print("Runtime: ",run8)
print("Runtime: ",run9)
print("Runtime: ",run10)

print("Runtime: ",runTime)


#res = spark.sql(sqlString)
res.show()
queryStopTime = datetime.now()
runTime = queryStopTime-queryStartTime 

#print("Runtime: ",runTime)
