from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf
spark.sparkContext.setLogLevel("WARN")

Logger= spark._jvm.org.apache.log4j.Logger
mylogger = Logger.getLogger("DAG")


def check(path, table):
    df = spark.read.parquet(path).filter("i94_dt = '{}'".format(month_year))
    if len(df.columns) > 0 and df.count() > 0:
        mylogger.warn("{} SUCCESS".format(table))
    else:
        mylogger.warn("{} FAIL".format(table))



check("s3a://capstone2021data/lake/immigrant/", "immigrant")
check("s3a://capstone2021data/lake/immigration/", "immigration")
check("s3a://capstone2021data/lake/immigration_demographic/", "immigration_demographic")