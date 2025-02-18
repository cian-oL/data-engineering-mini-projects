import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1739908698100 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1739908698100",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1739909632530 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1739909632530",
)

# Script generated for node Join
Join_node1739909707881 = Join.apply(
    frame1=AccelerometerTrusted_node1739909632530,
    frame2=StepTrainerTrusted_node1739908698100,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1739909707881",
)

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(
    frame=Join_node1739909707881,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1739909271228",
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL",
    },
)
MachineLearningCurated_node1739909915399 = glueContext.getSink(
    path="s3://my-udacity-stedi/step-trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node1739909915399",
)
MachineLearningCurated_node1739909915399.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node1739909915399.setFormat("json")
MachineLearningCurated_node1739909915399.writeFrame(Join_node1739909707881)
job.commit()
