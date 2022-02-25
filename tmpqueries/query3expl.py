from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType
from datetime import date, time, datetime

spark = SparkSession.builder.appName("Query3_DFAPI").master("yarn").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.join.preferSortMergeJoin", False)


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

#lz = lineitem.rdd.glom().map(len).collect()
#l = orders.rdd.glom().map(len).collect()  # get length of each partition
#l1 = customer.rdd.glom().map(len).collect()

#tmp = customer.rdd.getNumPartitions()
#tmp2 = orders.rdd.getNumPartitions()
#tmp3 = lineitem.rdd.getNumPartitions()
queryStartTime = datetime.now()
res = spark.sql(sqlString).explain(True)
queryStopTime = datetime.now()
runTime = queryStopTime-queryStartTime
res.show()

#print("Runtime: ",runTime)
#print("Customer partitions: ", tmp)
#print("Orders partitions: ", tmp2)
#print("Lineitem partitions: ", tmp3)

#print('Min Partition Size: ', min(lz), '. Max Partition Size: ', max(lz), '. Avg Partition Size: ', sum(lz)/len(lz), '. Total Partitions: ', len(lz))
#print('Min Parition Size: ',min(l),'. Max Parition Size: ', max(l),'. Avg Parition Size: ', sum(l)/len(l),'. Total Partitions: ', len(l))
#print('Min Partition Size: ', min(l1), '. Max Partition Size: ', max(l1), '. Avg Partition Size: ', sum(l1)/len(l1), '. Total Partitions: ', len(l1))

print("Runtime: ",runTime)

