from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import BooleanType, StructField,StructType, IntegerType, DateType, LongType, DoubleType, StringType
from datetime import date, time, datetime
import sparkSession.experimental.extraStrategies

#class GroupByPushDown(spark: SparkSession) extends Rule[LogicalPlan] {
#def apply(plan: LogicalPlan): LogicalPlan = plan transform { ... }

#}

#type ExtensionsBuilder = SparkSessionExtensions => Unit
#val f: ExtensionsBuilder = { e => e.injectOptimizerRule(GroupByPushDown)}

#object MyStrategy extends Strategy {
#  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
#    println("Hello world!")
#    Nil
#  }
#}

#spark = SparkSession.builder.appName("Query1_DFAPI").master("yarn").withExtensions(f).getOrCreate()
val spark = SparkSession.builder.appName("Query1_DFAPI").getOrCreate()

#spark.experimental.extraStrategies = Seq(MyStrategy)
#val q = spark.catalog.listTables.filter(t => t.name == "five")
#q.explain(true)
#spark.stop()

fileSchema = spark.read.options(header="true", inferSchema = "true").csv("/user/diplomma/data/schema/lineitem.csv").schema
lineitem = spark.read.format("csv").options(inferSchema = "true" , header = "false").schema(fileSchema).option("delimiter", "|").csv("/user/diplomma/data/data/lineitem.tbl")

lineitem.registerTempTable("lineitem")
queryStartTime = datetime.now()
sqlString= "select \
        l_returnflag, \
        l_linestatus, \
        sum(l_quantity) as sum_qty, \
        sum(l_extendedprice) as sum_base_price, \
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, \
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, \
        avg(l_quantity) as avg_qty, \
        avg(l_extendedprice) as avg_price, \
        avg(l_discount) as avg_disc, \
        count(*) as count_order \
from \
        lineitem \
where \
        l_shipdate <= date '1998-12-01' - interval '90' day \
group by \
        l_returnflag, \
        l_linestatus \
order by \
        l_returnflag, \
        l_linestatus; \
"

val res = spark.sql(sqlString)
queryStopTime = datetime.now()
runTime = queryStopTime-queryStartTime 
res.show()

print("Runtime: ",runTime)


