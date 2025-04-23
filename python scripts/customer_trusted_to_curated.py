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

# Script generated for node customer_trusted
customer_trusted_node1745350477041 = glueContext.create_dynamic_frame.from_catalog(database="stedi_lakehouse", table_name="customer_trusted", transformation_ctx="customer_trusted_node1745350477041")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1745350516609 = glueContext.create_dynamic_frame.from_catalog(database="stedi_lakehouse", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1745350516609")

# Script generated for node Join
Join_node1745350552957 = Join.apply(frame1=customer_trusted_node1745350477041, frame2=accelerometer_trusted_node1745350516609, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1745350552957")

# Script generated for node SQL Query
SqlQuery1268 = '''
SELECT DISTINCT c.email, c.serialNumber, c.birthDay, c.registrationDate, c.shareWithResearchAsOfDate
FROM myDataSource c

'''
SQLQuery_node1745390045976 = sparkSqlQuery(glueContext, query = SqlQuery1268, mapping = {"myDataSource":Join_node1745350552957}, transformation_ctx = "SQLQuery_node1745390045976")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1745390045976, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745349797140", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1745350893878 = glueContext.getSink(path="s3://glue-bucket-varshini/customer_landing/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1745350893878")
AmazonS3_node1745350893878.setCatalogInfo(catalogDatabase="stedi_lakehouse",catalogTableName="customer_curated")
AmazonS3_node1745350893878.setFormat("json")
AmazonS3_node1745350893878.writeFrame(SQLQuery_node1745390045976)
job.commit()