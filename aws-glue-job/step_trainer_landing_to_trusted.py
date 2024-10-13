import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node CustomerCurated
CustomerCurated_node1728795039147 = glueContext.create_dynamic_frame.from_catalog(database="thaovi_lake_house_db", table_name="customer_curated", transformation_ctx="CustomerCurated_node1728795039147")

# Script generated for node StepTrainnerLanding
StepTrainnerLanding_node1728795040014 = glueContext.create_dynamic_frame.from_catalog(database="thaovi_lake_house_db", table_name="step_trainer_landing", transformation_ctx="StepTrainnerLanding_node1728795040014")

# Script generated for node JoinAndFilterStepTrainer
SqlQuery9395 = '''
SELECT step.sensorReadingTime, step.serialNumber, step.distanceFromObject
FROM customer_curated AS cus
INNER JOIN step_trainer_landing AS step
ON cus.serialNumber = step.serialNumber;

'''
JoinAndFilterStepTrainer_node1728795042263 = sparkSqlQuery(glueContext, query = SqlQuery9395, mapping = {"step_trainer_landing":StepTrainnerLanding_node1728795040014, "customer_curated":CustomerCurated_node1728795039147}, transformation_ctx = "JoinAndFilterStepTrainer_node1728795042263")

# Script generated for node StepTrainerTrusted
StepTrainerTrusted_node1728795046211 = glueContext.getSink(path="s3://thaovi-lake-house/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1728795046211")
StepTrainerTrusted_node1728795046211.setCatalogInfo(catalogDatabase="thaovi_lake_house_db",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1728795046211.setFormat("json")
StepTrainerTrusted_node1728795046211.writeFrame(JoinAndFilterStepTrainer_node1728795042263)
job.commit()