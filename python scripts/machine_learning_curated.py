import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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
AWSGlueDataCatalog_node1745399060571 = glueContext.create_dynamic_frame.from_catalog(database="stedi_lakehouse", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1745399060571")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1745399058459 = glueContext.create_dynamic_frame.from_catalog(database="stedi_lakehouse", table_name="step_trainer_trusted", transformation_ctx="AWSGlueDataCatalog_node1745399058459")

# Script generated for node Join
Join_node1745399104009 = Join.apply(frame1=AWSGlueDataCatalog_node1745399058459, frame2=AWSGlueDataCatalog_node1745399060571, keys1=["sensorreadingtime"], keys2=["timestamp"], transformation_ctx="Join_node1745399104009")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1745399104009, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745399026070", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1745399182604 = glueContext.getSink(path="s3://glue-bucket-varshini/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1745399182604")
AmazonS3_node1745399182604.setCatalogInfo(catalogDatabase="stedi_lakehouse",catalogTableName="machine_learning_curated")
AmazonS3_node1745399182604.setFormat("json")
AmazonS3_node1745399182604.writeFrame(Join_node1745399104009)
job.commit()
