import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "parquet", table_name = "raw_data", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "parquet", table_name = "raw_data", transformation_ctx = "datasource0")

datasource0_df = datasource0.toDF()

# Apply a lambda to remove the '$'
chop_f = udf(lambda x: x.strip(), StringType())

datasource0_df = datasource0_df.withColumn("col2", chop_f(datasource0_df["col2"])).withColumn("col20", chop_f(datasource0_df["col20"]))

datasource0_df = datasource0_df.na.replace(['False.', 'True.'], ['0', '1'], 'col20')

# Turn it back to a dynamic frame
datasource0 = DynamicFrame.fromDF(datasource0_df, glueContext, "nested")

datasource0 = datasource0.resolveChoice(specs = [('col20','cast:int')])

## @type: ApplyMapping
## @args: [mapping = [("col3", "string", "phone_number", "string"), ("col4", "string", "col4", "string"), ("col5", "string", "col5", "string"), ("col6", "long", "col6", "long"), ("col7", "double", "col7", "double"), ("col2", "long", "area_code", "int"), ("col9", "double", "col9", "double"), ("col1", "long", "account_length", "int"), ("col11", "long", "col11", "long"), ("col12", "double", "col12", "double"), ("col13", "double", "col13", "double"), ("col14", "long", "col14", "long"), ("col15", "double", "col15", "double"), ("col16", "double", "col16", "double"), ("col17", "long", "col17", "long"), ("col20", "string", "churn", "boolean"), ("col19", "long", "col19", "long"), ("col0", "string", "state", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("col3", "string", "phone_number", "string"), ("col2", "string", "area_code", "int"), ("col20", "int", "churn", "boolean"), ("col0", "string", "state", "string")], transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["state", "account_length", "area_code", "phone_number", "churn"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["state", "account_length", "area_code", "phone_number", "churn"], transformation_ctx = "selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "parquet", table_name = "churn", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]


resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "parquet", table_name = "churn", transformation_ctx = "resolvechoice3")
## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_struct", transformation_ctx = "resolvechoice4")
## @type: DataSink
## @args: [database = "parquet", table_name = "churn", transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice4, database = "parquet", table_name = "churn", transformation_ctx = "datasink5")
job.commit()