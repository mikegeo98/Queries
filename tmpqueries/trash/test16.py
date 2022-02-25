from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

spark = SparkSession.builder.appName("Query1_DFAPI").getOrCreate()

nationschema= StructType([
  StructField("n_nationkey", IntegerType()),
  StructField("n_name",StringType()),
  StructField("n_regionkey",IntegerType()),
  StructField("n_comment",StringType())])


regionschema= StructType([
  StructField("r_regionkey", IntegerType()),
  StructField("r_name",StringType()),
  StructField("r_comment",StringType())])

partschema= StructType([
  StructField("p_partkey", LongType()),
  StructField("p_name",StringType()),
  StructField("p_mfgr", StringType()),
  StructField("p_brand",StringType()),
  StructField("p_type", StringType()),
  StructField("p_size",IntegerType()),
  StructField("p_container", StringType()),
  StructField("p_retailprice",DoubleType()),
  StructField("p_comment",StringType())])

supplierschema= StructType([
  StructField("s_suppkey", LongType()),
  StructField("s_name",StringType()),
  StructField("s_address", StringType()),
  StructField("s_nationkey",IntegerType()),
  StructField("s_phone", StringType()),
  StructField("s_acctbal",DoubleType()),
  StructField("s_comment",StringType())])

partsuppschema= StructType([
  StructField("ps_partkey", LongType()),
  StructField("ps_suppkey",LongType()),
  StructField("ps_availqty", LongType()),
  StructField("ps_supplycost",DoubleType()),
  StructField("ps_comment",StringType())])

customerschema= StructType([
  StructField("c_custkey", LongType()),
  StructField("c_name",StringType()),
  StructField("c_address", StringType()),
  StructField("c_nationkey",IntegerType()),
  StructField("c_phone",StringType()),
  StructField("c_acctbal", DoubleType()),
  StructField("c_mktsegment",StringType()),
  StructField("c_comment",StringType())])

ordersschema= StructType([
  StructField("o_orderkey", LongType()),
  StructField("o_custkey",LongType()),
  StructField("o_orderstatus", LongType()),
  StructField("o_totalprice",DoubleType()),
  StructField("o_orderdate",DateType()),
  StructField("o_orderpriority", StringType()),
  StructField("o_clerk",StringType()),
  StructField("o_shippriority",IntegerType()),
  StructField("o_comment",StringType())])



lineitemschema= StructType([
  StructField("l_orderkey",LongType()), \
  StructField("l_partkey",LongType()), \
  StructField("l_suppkey",LongType()), \
  StructField("l_linenumber",LongType()), \
  StructField("l_quantity",DoubleType()), \
  StructField("l_extendedprice",DoubleType()), \
  StructField("l_discount",DoubleType()), \
  StructField("l_tax",DoubleType()), \
  StructField("l_returnflag",BooleanType()), \
  StructField("l_linestatus",BooleanType()), \
  StructField("l_shipdate",DateType()), \
  StructField("l_commitdate",DateType()), \
  StructField("l_receiptdate",DateType()), \
  StructField("l_shipinstruct",StringType()), \
  StructField("l_shipmode",StringType()), \
  StructField("l_comment",StringType())])

nation = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(nationschema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/nation.tbl")
region = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(regionschema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/region.tbl")
part = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partschema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/part.tbl")
supplier = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(supplierschema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/supplier.tbl")
partsupp = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partsuppschema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/partsupp.tbl")
customer = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(customerschema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/customer.tbl")
orders = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(ordersschema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/orders.tbl")

lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(lineitemschema).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/lineitem.tbl")
#lineitem = spark.read.format("csv").csv("/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/ref_data/1/lineitem.tbl.1")

nation.registerTempTable("nation")
region.registerTempTable("region")
part.registerTempTable("part")
supplier.registerTempTable("supplier")
partsupp.registerTempTable("partsupp")
customer.registerTempTable("customer")
orders.registerTempTable("orders")

lineitem.registerTempTable("lineitem")

#sqlString = "select * from region"
sqlString= "select \
        p_brand, \
        p_type, \
        p_size, \
        count(distinct ps_suppkey) as supplier_cnt \
from \
        partsupp, \
        part \
where \
        p_partkey = ps_partkey \
        and p_brand <> 'Brand#45' \
        and p_type not like 'MEDIUM POLISHED%' \
        and p_size in (49, 14, 23, 45, 19, 3, 36, 9) \
        and ps_suppkey not in ( \
                select \
                        s_suppkey \
                from \
                        supplier \
                where \
                        s_comment like '%Customer%Complaints%' \
        ) \
group by \
        p_brand, \
        p_type, \
        p_size \
order by \
        supplier_cnt desc, \
        p_brand, \
        p_type, \
        p_size;"



res = spark.sql(sqlString)
res.show()


