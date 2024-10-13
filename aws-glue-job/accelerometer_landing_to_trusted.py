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

# Script generated for node CustomerTrusted
CustomerTrusted_node1728779831603 = glueContext.create_dynamic_frame.from_catalog(database="thaovi_lake_house_db", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1728779831603")

# Script generated for node AccelerometerLanding
AccelerometerLanding_node1728779813737 = glueContext.create_dynamic_frame.from_catalog(database="thaovi_lake_house_db", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1728779813737")

# Script generated for node JoinAndFilterAccelerometerLanding
SqlQuery2297 = '''
select a.user, a.timestamp, a.x, a.y, a.z from customerTrusted c
inner join acceLanding a
on c.email = a.user;
'''
JoinAndFilterAccelerometerLanding_node1728798448365 = sparkSqlQuery(glueContext, query = SqlQuery2297, mapping = {"customerTrusted":CustomerTrusted_node1728779831603, "acceLanding":AccelerometerLanding_node1728779813737}, transformation_ctx = "JoinAndFilterAccelerometerLanding_node1728798448365")

# Script generated for node AcccelerometerTrusted
AcccelerometerTrusted_node1728779923794 = glueContext.getSink(path="s3://thaovi-lake-house/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AcccelerometerTrusted_node1728779923794")
AcccelerometerTrusted_node1728779923794.setCatalogInfo(catalogDatabase="thaovi_lake_house_db",catalogTableName="accelerometer_trusted")
AcccelerometerTrusted_node1728779923794.setFormat("json")
AcccelerometerTrusted_node1728779923794.writeFrame(JoinAndFilterAccelerometerLanding_node1728798448365)
job.commit()