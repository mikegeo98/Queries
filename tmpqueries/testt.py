from pyspark.sql import SparkSession 
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType

spark = SparkSession.builder.appName("Query1_DFAPI").getOrCreate()

nation = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema.py).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/nation.tbl")
supplier = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema.py).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/supplier.tbl")
partsupp = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(/home/mikeg/Documents/databases/TPC-H/TPC-H_Tools_v3.0.0/dbgen/tmpqueries/schema.py).option("delimiter", "|").csv("hdfs://localhost:9000/user/diplomma/data/data/partsupp.tbl")

nation.registerTempTable("nation")
supplier.registerTempTable("supplier")
partsupp.registerTempTable("partsupp")

#sqlString = "select * from region"
sqlString="select \
        ps_partkey, \
        sum(ps_supplycost * ps_availqty) as value \
from \
        partsupp, \
        supplier, \
        nation \
where \
        ps_suppkey = s_suppkey \
        and s_nationkey = n_nationkey \
        and n_name = 'GERMANY' \
group by \
        ps_partkey having \
                sum(ps_supplycost * ps_availqty) > ( \
                        select \
                                sum(ps_supplycost * ps_availqty) * 0.0001000000 \
                        from \
                                partsupp, \
                                supplier, \
                                nation \
                        where \
                                ps_suppkey = s_suppkey \
                                and s_nationkey = n_nationkey \
                                and n_name = 'GERMANY' \
                ) \
order by \
        value desc; \
"


res = spark.sql(sqlString)
res.show()


