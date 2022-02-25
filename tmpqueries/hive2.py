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

spark = SparkSession.builder.appName("Query3_DFAPI").config("spark.executor.instances","6").config("spark.executor.cores","4").master("yarn").config("spark.sql.warehouse.dir", warehouse_location).enableHiveSupport().getOrCreate()
#spark = SparkSession.builder.appName("Query3_DFAPI").getOrCreate()
#spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1024)
spark.conf.set("spark.sql.join.preferSortMergeJoin", False)



spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")

#spark.sql("ANALYZE TABLE part COMPUTE STATISTICS FOR COLUMNS p_partkey,p_size,p_type;")
#spark.sql("ANALYZE TABLE supplier COMPUTE STATISTICS FOR COLUMNS s_suppkey,s_nationkey,s_acctbal;")
#spark.sql("ANALYZE TABLE partsupp COMPUTE STATISTICS FOR COLUMNS ps_suppkey,ps_supplycost;")
#spark.sql("ANALYZE TABLE region COMPUTE STATISTICS FOR COLUMNS r_regionkey,r_name;")
#spark.sql("ANALYZE TABLE nation COMPUTE STATISTICS FOR COLUMNS n_regionkey,n_nationkey;")



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
        part, \
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


res = spark.sql(sqlString)
res.show()
spark.sql(sqlString).explain()

#queryStartTime = datetime.now()
#res = spark.sql(sqlString)
#queryStopTime = datetime.now()
#runTime = queryStopTime-queryStartTime 
#res.show()

#print("Runtime: ",runTime)


