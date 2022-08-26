import sys
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder\
.appName("EMRonEKS-DeltaTable")\
.enableHiveSupport()\
.getOrCreate()

if (len(sys.argv) != 2):
 print("Usage: [target-s3-bucket] ")
 sys.exit(0)
  
S3_BUCKET_NAME=sys.argv[1]
TABLE_LOCATION=f"s3://{S3_BUCKET_NAME}/delta/delta_contact"

# Defining the schema for the source data
contact_schema = StructType(
      [ StructField("id",IntegerType(),True), 
       StructField("name",StringType(),True), 
       StructField("email",StringType(),True), 
       StructField("state", StringType(), True)])

###############################
# Job1 - inital load (one-off)
###############################
# Read initial contact CSV file and create a Delta table with extra SCD2 columns
df_intial_csv = spark.read.schema(contact_schema)\
 .format("csv")\
 .options(header=False,delimiter=",")\
 .load(f"s3://{S3_BUCKET_NAME}/blog/data/initial_contacts.csv")\
 .withColumn("valid_from", lit(current_timestamp()).cast(TimestampType()))\
 .withColumn("valid_to", lit("").cast(TimestampType()))\
 .withColumn("iscurrent", lit(1).cast("int"))\
 .withColumn("checksum",md5(concat(col("name"),col("email"),col("state"))))\
 .write.format("delta")\
 .mode("overwrite")\
 .save(TABLE_LOCATION)

spark.sql(f"""CREATE TABLE IF NOT EXISTS delta_table_contact USING DELTA LOCATION '{TABLE_LOCATION}'""")
spark.sql("GENERATE symlink_format_manifest FOR TABLE delta_table_contact")
spark.sql("ALTER TABLE delta_table_contact SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)")

# Create a queriable table in Athena
spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS default.delta_contact (
     `id` int
    ,`name` string
    ,`email` string
    ,`state` string
    ,`valid_from` timestamp
    ,`valid_to` timestamp
    ,`iscurrent` int
    ,`checksum` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '{TABLE_LOCATION}/_symlink_format_manifest/'""")

##########################
# Job2 - incremental load
##########################
# Read incremental CSV file with extra SCD2 columns
spark.read.schema(contact_schema)\
.format("csv").options(header=False,delimiter=",")\
.load(f"s3://{S3_BUCKET_NAME}/blog/data/update_contacts.csv")\
.withColumn("valid_from", lit(current_timestamp()).cast(TimestampType())) \
.withColumn("valid_to", lit("").cast(TimestampType()))\
.withColumn("iscurrent",lit(1).cast("int")) \
.withColumn("checksum",md5(concat(col("name"),col("email"),col("state")))) \
.createOrReplaceTempView('staged_update')

# 'NULL' mergeKey ensures the overlapped rows to be inserted, and expire existing target records
contact_update_qry = """
    SELECT NULL AS mergeKey, source.*
    FROM delta_table_contact AS target
    INNER JOIN staged_update as source
    ON target.id = source.id
    WHERE target.checksum != source.checksum
      AND target.iscurrent = 1
  UNION
    SELECT id AS mergeKey, *
    FROM staged_update
"""
# Upsert
spark.sql(f"""
    MERGE INTO delta_table_contact tgt
    USING ({contact_update_qry}) src
    ON tgt.id = src.mergeKey
    WHEN MATCHED AND src.checksum != tgt.checksum AND tgt.iscurrent = 1 
      THEN UPDATE SET valid_to = src.valid_from, iscurrent = 0
    WHEN NOT MATCHED THEN INSERT *
""")