import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    df = dfc.select(list(dfc.keys())[0]).toDF()
    
    from pyspark.sql.functions import col, year, month, dayofmonth, to_date, from_unixtime

    repartitioned_with_new_columns_df = df.withColumn(“date_col”, to_date(from_unixtime(col(“datetime”)))).withColumn(“year”, year(col(“date_col”))).withColumn(“month”, month(col(“date_col”))).withColumn(“day”, dayofmonth(col(“date_col”))).drop(col(“date_col”)).repartition(1)

    dyf = DynamicFrame.fromDF(repartitioned_with_new_columns_df, glueContext, "enriched")
    return(DynamicFrameCollection({"enriched": dyf}, glueContext))

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "canlog", table_name = "canlog", transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "canlog", table_name = "canlog", transformation_ctx = "DataSource0")
## @type: ApplyMapping
## @args: [mappings = [("bus", "long", "bus", "tinyint"), ("messageid", "string", "messageid", "string"), ("message", "string", "message", "string"), ("messagelength", "long", "messagelength", "long"), ("datetime", "string", "datetime", "timestamp")], transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [frame = DataSource0]
Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("bus", "long", "bus", "tinyint"), ("messageid", "string", "messageid", "string"), ("message", "string", "message", "string"), ("messagelength", "long", "messagelength", "long"), ("datetime", "string", "datetime", "timestamp")], transformation_ctx = "Transform0")
## @type: CustomCode
## @args: [dynamicFrameConstruction = DynamicFrameCollection({"Transform0": Transform0}, glueContext), className = MyTransform, transformation_ctx = "Transform1"]
## @return: Transform1
## @inputs: [dfc = Transform0]
Transform1 = MyTransform(glueContext, DynamicFrameCollection({"Transform0": Transform0}, glueContext))
## @type: SelectFromCollection
## @args: [key = list(Transform1.keys())[0], transformation_ctx = "Transform2"]
## @return: Transform2
## @inputs: [dfc = Transform1]
Transform2 = SelectFromCollection.apply(dfc = Transform1, key = list(Transform1.keys())[0], transformation_ctx = "Transform2")
## @type: DataSink
## @args: [connection_type = "s3", format = "parquet", connection_options = {"path": "s3://badqueen-video-archive/canlog_data/", "compression": "gzip", "partitionKeys": ["year" ,"month" ,"day" ,"messageid"]}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform2]
DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform2, connection_type = "s3", format = "parquet", connection_options = {"path": "s3://badqueen-video-archive/canlog_data/", "compression": "gzip", "partitionKeys": ["year" ,"month" ,"day" ,"messageid"]}, transformation_ctx = "DataSink0")
job.commit()
