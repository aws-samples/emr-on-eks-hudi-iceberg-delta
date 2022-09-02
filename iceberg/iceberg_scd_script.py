import sys
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("EMRonEKS-IcebergTable").getOrCreate()
print(len(sys.argv))
if (len(sys.argv) != 2):
  print("Usage: [target-s3-bucket] ")
  sys.exit(0)
  
S3_BUCKET_NAME=sys.argv[1]
TABLE_LOCATION=f"s3://{S3_BUCKET_NAME}/iceberg"

# Defining the schema for the source data
contact_schema = StructType(
            [ StructField("id",IntegerType(),True), 
              StructField("name",StringType(),True), 
              StructField("email",StringType(),True), 
              StructField("state", StringType(), True)])

##########################
# Job1 - inital load
##########################
# Read initial contact CSV file and create Iceberg table in Glue catalog with extra SCD2 columns
df_intial_csv = spark.read.schema(contact_schema).\
format("csv").options(header=False,delimiter=",").\
load(f"s3://{S3_BUCKET_NAME}/blog/data/initial_contacts.csv")\
.withColumn("ts", lit(current_timestamp()).cast(TimestampType()))\
.withColumn("valid_from", lit(current_timestamp()).cast(TimestampType()))\
.withColumn("valid_to", lit("").cast(TimestampType()))\
.withColumn('iscurrent', lit(1).cast("int"))\
.withColumn("checksum",md5(concat(col("name"),col("email"),col("state"))))\
.writeTo("glue_catalog.default.iceberg_contact")\
.tableProperty("location", TABLE_LOCATION)\
.tableProperty("format-version", "2")\
.createOrReplace()

##########################
# Job2 - incremental load
##########################
## Read Updated Contact CSV file
delta_csv_df = spark.read.schema(contact_schema).\
format("csv").options(header=False,delimiter=",").\
load(f"s3://{S3_BUCKET_NAME}/blog/data/update_contacts.csv")\
.withColumn("ts", lit(current_timestamp()).cast(TimestampType()))\
.withColumn("valid_from", lit(current_timestamp()).cast(TimestampType()))\
.withColumn("valid_to", lit("").cast(TimestampType()))\
.withColumn("iscurrent",lit(1).cast("int")) \
.withColumn("checksum",md5(concat(col("name"),col("email"),col("state"))))\
.createOrReplaceTempView('iceberg_contact_update')

# Update existing records which are changed in the update file
contact_update_qry = """
    WITH contact_to_update AS (
          SELECT target.*
          FROM glue_catalog.default.iceberg_contact AS target
          JOIN iceberg_contact_update AS source 
          ON target.id = source.id
          WHERE target.checksum != source.checksum
            AND target.iscurrent = 1
        UNION
          SELECT * FROM iceberg_contact_update
    ),contact_updated AS (
        SELECT *, LEAD(valid_from) OVER (PARTITION BY id ORDER BY valid_from) AS eff_from
        FROM contact_to_update
    )
    SELECT id,name,email,state,ts,valid_from,
        CAST(COALESCE(eff_from, null) AS Timestamp) AS valid_to,
        CASE WHEN eff_from IS NULL THEN 1 ELSE 0 END AS iscurrent,
        checksum
    FROM contact_updated
"""
# Upsert
spark.sql(f"""
    MERGE INTO glue_catalog.default.iceberg_contact tgt
    USING ({contact_update_qry}) src
    ON tgt.id = src.id
    AND tgt.checksum = src.checksum
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")