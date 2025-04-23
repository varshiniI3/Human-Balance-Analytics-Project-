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

# Script generated for node accelerometer_landing
accelerometer_landing_node1745348953084 = glueContext.create_dynamic_frame.from_catalog(database="stedi_lakehouse", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1745348953084")

# Script generated for node customer_trusted
customer_trusted_node1745349408222 = glueContext.create_dynamic_frame.from_catalog(database="stedi_lakehouse", table_name="customer_trusted", transformation_ctx="customer_trusted_node1745349408222")

# Script generated for node JoinAccelCustomer
JoinAccelCustomer_node1745349486528 = Join.apply(frame1=accelerometer_landing_node1745348953084, frame2=customer_trusted_node1745349408222, keys1=["user"], keys2=["email"], transformation_ctx="JoinAccelCustomer_node1745349486528")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=JoinAccelCustomer_node1745349486528, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745349383483", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1745349581652 = glueContext.getSink(path="s3://glue-bucket-varshini/accelerometer_landing/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1745349581652")
AmazonS3_node1745349581652.setCatalogInfo(catalogDatabase="stedi_lakehouse",catalogTableName="accelerometer_trusted")
AmazonS3_node1745349581652.setFormat("json")
AmazonS3_node1745349581652.writeFrame(JoinAccelCustomer_node1745349486528)
job.commit()