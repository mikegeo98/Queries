from os.path import abspath
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType
from datetime import date, time, datetime

#spark = SparkSession.builder.appName("Query2_DFAPI").getOrCreate()

#partSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/part.csv").schema
#part = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partSchema).option("delimiter", "|").csv("/user/diplomma/data/data/part.tbl")
#part.registerTempTable("part")

#supplierSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/supplier.csv").schema
#supplier = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(supplierSchema).option("delimiter", "|").csv("/user/diplomma/data/data/supplier.tbl")
#supplier.registerTempTable("supplier")

#partsuppSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/partsupp.csv").schema
#partsupp = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partsuppSchema).option("delimiter", "|").csv("/user/diplomma/data/data/partsupp.tbl")
#partsupp.registerTempTable("partsupp")

#nationSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/nation.csv").schema
#nation = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(nationSchema).option("delimiter", "|").csv("/user/diplomma/data/data/nation.tbl")
#nation.registerTempTable("nation")

#regionSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/region.csv").schema
#region = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(regionSchema).option("delimiter", "|").csv("/user/diplomma/data/data/region.tbl")
#region.registerTempTable("region")

warehouse_location = abspath('spark-warehouse')

#spark = SparkSession.builder.appName("Query2_DFAPI").master("yarn").config("spark.sql.warehouse.dir", warehouse_location).enableHiveSupport().getOrCreate()
spark = SparkSession.builder.appName("Query3_DFAPI").config("spark.executor.instances","4").config("spark.executor.cores","4").master("yarn").config("spark.sql.warehouse.dir", warehouse_location).enableHiveSupport().getOrCreate()
#spark = SparkSession.builder.appName("Query3_DFAPI").getOrCreate()
#spark.conf.get("spark.sql.autoBroadcastJoinThreshold","10240000000")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1024000)
spark.conf.set("spark.sql.join.preferSortMergeJoin", False)



spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")

#spark.sql("ANALYZE TABLE lineitem COMPUTE STATISTICS FOR COLUMNS l_orderkey,l_returnflag;")
spark.sql("ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS o_custkey,o_orderkey,o_orderdate;")
spark.sql("ANALYZE TABLE customer COMPUTE STATISTICS FOR COLUMNS c_custkey,c_nationkey;")


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
	and c_custkey < 150000 \
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


sqlString4 ="select \
        c_custkey, \
        o_orderdate, \
        o_shippriority \
from \
        customer,\
        orders \
where \
        c_mktsegment = 'BUILDING' \
        and c_custkey = o_custkey \
        and o_orderdate < date '1995-03-15';\
"


#tmp = customer.rdd.getNumPartitions()
#tmp2 = orders.rdd.getNumPartitions()
#tmp3 = lineitem.rdd.getNumPartitions()
res = spark.sql(sqlString)
res.show()
spark.sql(sqlString).explain()
#print("Partitions no customer orders lineitem",tmp,tmp2,tmp3)

#queryStartTime = datetime.now()
#res = spark.sql(sqlString)
#queryStopTime = datetime.now()
#runTime = queryStopTime-queryStartTime 
#res.show()

#print("Runtime: ",runTime)


