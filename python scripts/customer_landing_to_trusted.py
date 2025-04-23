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

# Script generated for node customer_landing
customer_landing_node1745348342107 = glueContext.create_dynamic_frame.from_catalog(database="stedi_lakehouse", table_name="customer_landing", transformation_ctx="customer_landing_node1745348342107")

# Script generated for node FilterSharedCustomers
SqlQuery1109 = '''
SELECT * 
FROM myDataSource 
WHERE sharewithresearchasofdate IS NOT NULL

'''
FilterSharedCustomers_node1745348508764 = sparkSqlQuery(glueContext, query = SqlQuery1109, mapping = {"myDataSource":customer_landing_node1745348342107}, transformation_ctx = "FilterSharedCustomers_node1745348508764")

# Script generated for node customer_trusted_output
EvaluateDataQuality().process_rows(frame=FilterSharedCustomers_node1745348508764, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745348193993", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_trusted_output_node1745348649962 = glueContext.getSink(path="s3://glue-bucket-varshini/customer_landing/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_output_node1745348649962")
customer_trusted_output_node1745348649962.setCatalogInfo(catalogDatabase="stedi_lakehouse",catalogTableName="customer_trusted")
customer_trusted_output_node1745348649962.setFormat("json")
customer_trusted_output_node1745348649962.writeFrame(FilterSharedCustomers_node1745348508764)
job.commit()