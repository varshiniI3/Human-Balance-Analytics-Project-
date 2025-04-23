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

# Script generated for node step_trainer_landing
step_trainer_landing_node1745398036708 = glueContext.create_dynamic_frame.from_catalog(database="stedi_lakehouse", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1745398036708")

# Script generated for node customer_curated
customer_curated_node1745398087644 = glueContext.create_dynamic_frame.from_catalog(database="stedi_lakehouse", table_name="customer_curated", transformation_ctx="customer_curated_node1745398087644")

# Script generated for node Join
Join_node1745398115155 = Join.apply(frame1=step_trainer_landing_node1745398036708, frame2=customer_curated_node1745398087644, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="Join_node1745398115155")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1745398115155, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745397828859", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1745398246438 = glueContext.getSink(path="s3://glue-bucket-varshini/step_trainer_landing/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1745398246438")
AmazonS3_node1745398246438.setCatalogInfo(catalogDatabase="stedi_lakehouse",catalogTableName="step_trainer_trusted")
AmazonS3_node1745398246438.setFormat("json")
AmazonS3_node1745398246438.writeFrame(Join_node1745398115155)
job.commit()