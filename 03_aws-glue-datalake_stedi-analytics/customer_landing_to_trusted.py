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

# Script generated for node Customer Landing
CustomerLanding_node1739881416596 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="CustomerLanding_node1739881416596")

# Script generated for node Share with Research
SqlQuery4793 = '''
select * from myDataSource
where sharewithresearchasofdate is not null
'''
SharewithResearch_node1739881976940 = sparkSqlQuery(glueContext, query = SqlQuery4793, mapping = {"myDataSource":CustomerLanding_node1739881416596}, transformation_ctx = "SharewithResearch_node1739881976940")

# Script generated for node Customer Trusted
EvaluateDataQuality().process_rows(frame=SharewithResearch_node1739881976940, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739878359261", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrusted_node1739882075295 = glueContext.getSink(path="s3://my-udacity-stedi/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1739882075295")
CustomerTrusted_node1739882075295.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
CustomerTrusted_node1739882075295.setFormat("json")
CustomerTrusted_node1739882075295.writeFrame(SharewithResearch_node1739881976940)
job.commit()