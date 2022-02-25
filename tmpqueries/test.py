from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType
from datetime import date, time, datetime

spark = SparkSession.builder.appName("test_DFAPI").config("spark.executor.instances","4").config("spark.executor.cores","4").master("yarn").getOrCreate()
spark.conf.set("spark.sql.join.preferSortMergeJoin", False)
spark.conf.get("spark.sql.autoBroadcastJoinThreshold","100000000")
#spark = SparkSession.builder.appName("Query1_DFAPI").getOrCreate()

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("/user/diplomma/data/data100/lineitem.tbl")

regionSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/region.csv").schema
region = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(regionSchema).option("delimiter", "|").csv("/user/diplomma/data/data100/region.tbl")

orderSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/orders.csv").schema
orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(orderSchema).option("delimiter", "|").csv("/user/diplomma/data/data100/orders.tbl")

customerSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/customer.csv").schema
customer = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(customerSchema).option("delimiter", "|").csv("/user/diplomma/data/data100/customer.tbl")

nationSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/nation.csv").schema
nation = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(nationSchema).option("delimiter", "|").csv("/user/diplomma/data/data100/nation.tbl")

nation.registerTempTable("nation")
customer.registerTempTable("customer")
region.registerTempTable("region")
lineitem.registerTempTable("lineitem")
orders.registerTempTable("orders")
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
sqlString = " select * \
from \
	lineitem \
"
sqlString11 = "select \
	n_name,r_name \
from \
	nation,region \
where \
	n_regionkey = r_regionkey;\
"
sqlString4 ="select \
        c_custkey, \
        o_orderdate, \
        o_shippriority \
from \
        customer,\
        orders \
where \
        c_mktsegment = 'BUILDING' \
	and c_custkey = 1 \
        and c_custkey = o_custkey \
        and o_orderdate < date '1995-03-15';\
"

sqlString5 = "select \
        l_orderkey, \
	o_orderdate, \
        o_shippriority \
from \
        orders, \
        lineitem \
where \
        l_orderkey = o_orderkey \
        and o_orderdate < date '1995-03-15'\
        and l_shipdate > date '1995-03-15';\
"
#        sum(l_extendedprice * (1 - l_discount)) as revenue, \
#group by \
#        l_orderkey, \
#        o_orderdate, \
#        o_shippriority \
#order by \
#        revenue desc, \
#        o_orderdate; "


#queryStartTime = datetime.now()
res = spark.sql(sqlString4)
#queryStopTime = datetime.now()
res.show()
spark.sql(sqlString4).explain()

res = spark.sql(sqlString11)
#queryStopTime = datetime.now()
res.show()
spark.sql(sqlString11).explain()

#runTime = queryStopTime-queryStartTime

#print("Runtime: ",runTime)
