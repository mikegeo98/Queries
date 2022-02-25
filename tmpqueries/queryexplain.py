from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType
from datetime import date, time, datetime

spark = SparkSession.builder.appName("Query2_DFAPI").getOrCreate()

spark.conf.set("spark.sql.cbo.enabled", True)

partSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/part.csv").schema
part = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partSchema).option("delimiter", "|").csv("/user/diplomma/data/data/part.tbl")
#part.registerTempTable("part")
part.saveAsTable("part")

supplierSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/supplier.csv").schema
supplier = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(supplierSchema).option("delimiter", "|").csv("/user/diplomma/data/data/supplier.tbl")
supplier.registerTempTable("supplier")

partsuppSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/partsupp.csv").schema
partsupp = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(partsuppSchema).option("delimiter", "|").csv("/user/diplomma/data/data/partsupp.tbl")
partsupp.registerTempTable("partsupp")

nationSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/nation.csv").schema
nation = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(nationSchema).option("delimiter", "|").csv("/user/diplomma/data/data/nation.tbl")
nation.registerTempTable("nation")

regionSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/region.csv").schema
region = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(regionSchema).option("delimiter", "|").csv("/user/diplomma/data/data/region.tbl")
region.registerTempTable("region")




spark.sql("ANALYZE TABLE parta COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE supplier COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE partsupp COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE nation COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE region COMPUTE STATISTICS")

sqlString="select \
        s_acctbal, \
        s_name, \
        n_name, \
        p_partkey, \
        p_mfgr, \
        s_address, \
        s_phone, \
        s_comment \
from \
        parta, \
        supplier, \
        partsupp, \
        nation, \
        region \
where \
        p_partkey = ps_partkey \
        and s_suppkey = ps_suppkey \
        and p_size = 15 \
        and p_type like '%BRASS' \
        and s_nationkey = n_nationkey \
        and n_regionkey = r_regionkey \
        and r_name = 'EUROPE' \
        and ps_supplycost = ( \
                select \
                        min(ps_supplycost) \
                from \
                        partsupp, \
                        supplier, \
                        nation, \
                        region \
                where \
                        p_partkey = ps_partkey \
                        and s_suppkey = ps_suppkey \
                        and s_nationkey = n_nationkey \
                        and n_regionkey = r_regionkey \
                        and r_name = 'EUROPE' \
        ) \
order by \
        s_acctbal desc, \
        n_name, \
        s_name, \
        p_partkey; \
"

queryStartTime = datetime.now()
res = spark.sql(sqlString).explain(True)
queryStopTime = datetime.now()
runTime = queryStopTime-queryStartTime 
res.show()

print("Runtime: ",runTime)


