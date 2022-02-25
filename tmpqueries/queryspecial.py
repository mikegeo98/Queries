from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType
from datetime import date, time, datetime

#spark = SparkSession.builder.appName("Query3_DFAPI").config("spark.driver.memory","8g").config("spark.driver.memory.overhead","1g").config("yarn.nodemanager.resource.memory-mb","6144").config("spark.executor.instances", "8").config("spark.executor.pyspark.memory","1g").config("spark.yarn.am.memory","6g").master("yarn").getOrCreate()
#spark = SparkSession.builder.appName("Query3_DFAPI").config("spark.executor.instances", "6").config("spark.executor.pyspark.memory","256m").config("spark.yarn.am.memory","3g").master("yarn").getOrCreate()
#spark = SparkSession.builder.appName("Query3_DFAPI").config("spark.yarn.am.memory","10240").config("spark.executor.memory","2g").config("spark.executor.pyspark.memory","1g").config("spark.executor.instances", "1").config("spark.executor.cores","1").master("yarn").getOrCreate()
spark = SparkSession.builder.appName("Query3_DFAPI").config("spark.executor.instances","1").config("spark.executor.cores","1").master("yarn").getOrCreate()
#spark = SparkSession.builder.appName("Query3_DFAPI").getOrCreate()
spark.conf.set("spark.sql.join.preferSortMergeJoin", False)
print("We are here")
SparkConf().getAll()

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("/user/diplomma/data/data10/lineitem.tbl")

orderSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/orders.csv").schema
orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(orderSchema).option("delimiter", "|").csv("/user/diplomma/data/data10/orders.tbl")

customerSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/customer.csv").schema
customer = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(customerSchema).option("delimiter", "|").csv("/user/diplomma/data/data10/customer.tbl")

lineitem.registerTempTable("lineitem")
orders.registerTempTable("orders")
customer.registerTempTable("customer")

#sum(l_extendedprice * (1 - l_discount)) as revenue,
sqlString="select \
                        extract(year from o_orderdate) as o_year, \
                        l_extendedprice * (1 - l_discount) as volume, \
                        n2.n_name as nation \
                from \
                        part, \
                        supplier, \
                        lineitem, \
                        orders, \
                        customer, \
                        nation n1, \
                        nation n2, \
                        region \
                where \
                        p_partkey = l_partkey \
                        and s_suppkey = l_suppkey \
                        and l_orderkey = o_orderkey \
                        and o_custkey = c_custkey \
                        and c_nationkey = n1.n_nationkey \
                        and n1.n_regionkey = r_regionkey \
                        and r_name = 'AMERICA' \
                        and s_nationkey = n2.n_nationkey \
                        and o_orderdate between date '1995-01-01' and date '1996-12-31' \
                        and p_type = 'ECONOMY ANODIZED STEEL';"

#lz = lineitem.rdd.glom().map(len).collect()
#l = orders.rdd.glom().map(len).collect()  # get length of each partition
#l1 = customer.rdd.glom().map(len).collect()

tmp = customer.rdd.getNumPartitions()
tmp2 = orders.rdd.getNumPartitions()
tmp3 = lineitem.rdd.getNumPartitions()
res = spark.sql(sqlString)
res.show()
spark.sql(sqlString).explain()
print("Partitions no customer orders lineitem",tmp,tmp2,tmp3)
