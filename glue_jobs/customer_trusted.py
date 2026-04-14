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

# Script generated for node Amazon S3
AmazonS3_node1776189052261 = glueContext.create_dynamic_frame.from_catalog(database="default", table_name="customer_landing", transformation_ctx="AmazonS3_node1776189052261")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT *
FROM customer_landing
WHERE shareWithResearchAsOfDate IS NOT NULL

'''
SQLQuery_node1776189147795 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AmazonS3_node1776189052261}, transformation_ctx = "SQLQuery_node1776189147795")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1776189147795, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1776188956606", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1776189167152 = glueContext.getSink(path="s3://stedi-lakehouse-earlgwin-090306607434-us-east-1-an/customer_trusted/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1776189167152")
AmazonS3_node1776189167152.setCatalogInfo(catalogDatabase="default",catalogTableName="customer_trusted")
AmazonS3_node1776189167152.setFormat("json")
AmazonS3_node1776189167152.writeFrame(SQLQuery_node1776189147795)
job.commit()