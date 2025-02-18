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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1739890418170 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1739890418170")

# Script generated for node Customer Trusted
CustomerTrusted_node1739890506788 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1739890506788")

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node1739890548070 = Join.apply(frame1=AccelerometerLanding_node1739890418170, frame2=CustomerTrusted_node1739890506788, keys1=["user"], keys2=["email"], transformation_ctx="CustomerPrivacyFilter_node1739890548070")

# Script generated for node Drop duplicates and Accelerometer Fields
SqlQuery4967 = '''
select distinct
    customername,
    email,
    phone,
    birthday,
    serialnumber,
    registrationdate,
    lastupdatedate,
    sharewithresearchasofdate,
    sharewithpublicasofdate,
    sharewithfriendsasofdate
from myDataSource
'''
DropduplicatesandAccelerometerFields_node1739895558560 = sparkSqlQuery(glueContext, query = SqlQuery4967, mapping = {"myDataSource":CustomerPrivacyFilter_node1739890548070}, transformation_ctx = "DropduplicatesandAccelerometerFields_node1739895558560")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=DropduplicatesandAccelerometerFields_node1739895558560, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739890315546", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1739890427020 = glueContext.getSink(path="s3://my-udacity-stedi/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1739890427020")
CustomerCurated_node1739890427020.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1739890427020.setFormat("json")
CustomerCurated_node1739890427020.writeFrame(DropduplicatesandAccelerometerFields_node1739895558560)
job.commit()