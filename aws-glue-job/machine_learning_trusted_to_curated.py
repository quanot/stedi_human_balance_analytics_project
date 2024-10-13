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

# Script generated for node StepTrainerTrusted
StepTrainerTrusted_node1728795039147 = glueContext.create_dynamic_frame.from_catalog(database="thaovi_lake_house_db", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1728795039147")

# Script generated for node AccelerometerTrusted
AccelerometerTrusted_node1728795040014 = glueContext.create_dynamic_frame.from_catalog(database="thaovi_lake_house_db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1728795040014")

# Script generated for node JoinAndFilterMachineLearning
SqlQuery8807 = '''
select a.user, a.x, a.y, a.z, s.sensorreadingtime,
    s.serialnumber, s.distancefromobject
from acceleromete_trusted a
inner join step_trusted s
on s.sensorReadingTime = a.timestamp
'''
JoinAndFilterMachineLearning_node1728795042263 = sparkSqlQuery(glueContext, query = SqlQuery8807, mapping = {"acceleromete_trusted":AccelerometerTrusted_node1728795040014, "step_trusted":StepTrainerTrusted_node1728795039147}, transformation_ctx = "JoinAndFilterMachineLearning_node1728795042263")

# Script generated for node MachineLearningCurated
MachineLearningCurated_node1728795046211 = glueContext.getSink(path="s3://thaovi-lake-house/machine-learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1728795046211")
MachineLearningCurated_node1728795046211.setCatalogInfo(catalogDatabase="thaovi_lake_house_db",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1728795046211.setFormat("json")
MachineLearningCurated_node1728795046211.writeFrame(JoinAndFilterMachineLearning_node1728795042263)
job.commit()