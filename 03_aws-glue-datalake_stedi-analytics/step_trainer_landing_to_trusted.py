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


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer Curated
CustomerCurated_node1739905752065 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1739905752065",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1739905723888 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="StepTrainerLanding_node1739905723888",
)

# Script generated for node Filter for unique matching serial numbers
SqlQuery0 = """
SELECT DISTINCT
    sensorreadingtime,
    serialnumber,
    distancefromobject
FROM stepTrainerLanding
WHERE stepTrainerLanding.serialnumber IN (
    SELECT DISTINCT serialnumber 
    FROM customerCurated
);
"""
Filterforuniquematchingserialnumbers_node1739906562603 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "stepTrainerLanding": StepTrainerLanding_node1739905723888,
        "customerCurated": CustomerCurated_node1739905752065,
    },
    transformation_ctx="Filterforuniquematchingserialnumbers_node1739906562603",
)

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(
    frame=Filterforuniquematchingserialnumbers_node1739906562603,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1739905202147",
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL",
    },
)
StepTrainerTrusted_node1739905974033 = glueContext.getSink(
    path="s3://my-udacity-stedi/step-trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node1739905974033",
)
StepTrainerTrusted_node1739905974033.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node1739905974033.setFormat("json")
StepTrainerTrusted_node1739905974033.writeFrame(
    Filterforuniquematchingserialnumbers_node1739906562603
)
job.commit()
