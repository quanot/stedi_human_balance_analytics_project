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

# Script generated for node Join
Join_node1728779851906 = Join.apply(frame1=AccelerometerLanding_node1728779813737, frame2=CustomerTrusted_node1728779831603, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1728779851906")

# Script generated for node DropFieldsAndDuplicate
SqlQuery8930 = '''
select distinct customername, email, phone, birthday, 
serialnumber, registrationdate, lastupdatedate, 
sharewithresearchasofdate, sharewithpublicasofdate,
sharewithfriendsasofdate from myDataSource
'''
DropFieldsAndDuplicate_node1728780557113 = sparkSqlQuery(glueContext, query = SqlQuery8930, mapping = {"myDataSource":Join_node1728779851906}, transformation_ctx = "DropFieldsAndDuplicate_node1728780557113")

# Script generated for node CustomerCurated
CustomerCurated_node1728779923794 = glueContext.getSink(path="s3://thaovi-lake-house/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1728779923794")
CustomerCurated_node1728779923794.setCatalogInfo(catalogDatabase="thaovi_lake_house_db",catalogTableName="customer_curated")
CustomerCurated_node1728779923794.setFormat("json")
CustomerCurated_node1728779923794.writeFrame(DropFieldsAndDuplicate_node1728780557113)
job.commit()