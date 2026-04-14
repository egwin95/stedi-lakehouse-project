import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1776201799147 = glueContext.create_dynamic_frame.from_catalog(database="default", table_name="step_trainer_trusted", transformation_ctx="AWSGlueDataCatalog_node1776201799147")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1776201808861 = glueContext.create_dynamic_frame.from_catalog(database="default", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1776201808861")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
    s.serialNumber,
    s.sensorReadingTime,
    s.distanceFromObject,
    a.x,
    a.y,
    a.z
FROM step_trainer_trusted s
JOIN accelerometer_trusted a
ON s.sensorReadingTime = a.timeStamp
'''
SQLQuery_node1776201819995 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AWSGlueDataCatalog_node1776201808861}, transformation_ctx = "SQLQuery_node1776201819995")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1776201819995, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1776201794928", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1776201830056 = glueContext.getSink(path="s3://stedi-lakehouse-earlgwin-090306607434-us-east-1-an/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1776201830056")
AmazonS3_node1776201830056.setCatalogInfo(catalogDatabase="default",catalogTableName="machine_learning_curated")
AmazonS3_node1776201830056.setFormat("json")
AmazonS3_node1776201830056.writeFrame(SQLQuery_node1776201819995)
job.commit()