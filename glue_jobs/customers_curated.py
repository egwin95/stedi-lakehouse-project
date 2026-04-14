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
AWSGlueDataCatalog_node1776201165124 = glueContext.create_dynamic_frame.from_catalog(database="default", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1776201165124")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1776201176199 = glueContext.create_dynamic_frame.from_catalog(database="default", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1776201176199")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT DISTINCT c.*
FROM customer_trusted c
JOIN accelerometer_trusted a
ON c.email = a.user
'''
SQLQuery_node1776201188875 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AWSGlueDataCatalog_node1776201176199}, transformation_ctx = "SQLQuery_node1776201188875")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1776201188875, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1776200969493", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1776201199885 = glueContext.getSink(path="s3://stedi-lakehouse-earlgwin-090306607434-us-east-1-an/customers_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1776201199885")
AmazonS3_node1776201199885.setCatalogInfo(catalogDatabase="default",catalogTableName="customers_curated")
AmazonS3_node1776201199885.setFormat("json")
AmazonS3_node1776201199885.writeFrame(SQLQuery_node1776201188875)
job.commit()