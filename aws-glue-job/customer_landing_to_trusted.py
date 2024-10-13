import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node CustomerLanding
CustomerLanding_node1728736937536 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://thaovi-lake-house/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1728736937536")

# Script generated for node CustomerLandingPrivacyFilter
CustomerLandingPrivacyFilter_node1728737022164 = Filter.apply(frame=CustomerLanding_node1728736937536, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="CustomerLandingPrivacyFilter_node1728737022164")

# Script generated for node CustomerTrusted
CustomerTrusted_node1728737254673 = glueContext.getSink(path="s3://thaovi-lake-house/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1728737254673")
CustomerTrusted_node1728737254673.setCatalogInfo(catalogDatabase="thaovi_lake_house_db",catalogTableName="customer_trusted")
CustomerTrusted_node1728737254673.setFormat("json")
CustomerTrusted_node1728737254673.writeFrame(CustomerLandingPrivacyFilter_node1728737022164)
job.commit()