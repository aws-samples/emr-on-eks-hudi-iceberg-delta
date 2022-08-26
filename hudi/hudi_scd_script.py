import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("EMRonEKS-HudiTable").getOrCreate()
if (len(sys.argv) != 4):
  print("Usage: [aws_region] [target-s3-bucket] [table-type-cow-or-mor] ")
  sys.exit(0)

REGION=sys.argv[1]
S3_BUCKET_NAME=sys.argv[2]
if sys.argv[3].upper() == 'COW':
  hudi_table_type =  'COPY_ON_WRITE'  
  hudi_table_name = 'hudi_contact_cow_table'
elif sys.argv[3].upper() == 'MOR':
  hudi_table_type =  'MERGE_ON_READ'  
  hudi_table_name = 'hudi_contact_mor_table'
else:
  print("Please specify the Table type as COW or MOR")
  sys.exit(0)

# common variables
contact_schema = StructType(
            [ StructField("id",IntegerType(),True), 
              StructField("name",StringType(),True), 
              StructField("email",StringType(),True), 
              StructField("state", StringType(), True)])

TABLE_LOCATION=f"s3://{S3_BUCKET_NAME}/hudi/" + hudi_table_name
hudiOptions = {
    "hoodie.table.name": hudi_table_name,
    "hoodie.datasource.write.table.type": hudi_table_type, # COW or MOR
    "hoodie.datasource.write.recordkey.field": "id,checksum", # a business key that represents SCD 
    "hoodie.datasource.hive_sync.support_timestamp": "true",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.hive_sync.table": hudi_table_name, # catalog table name
    "hoodie.datasource.hive_sync.database": "default", # catalog database name
    "hoodie.datasource.hive_sync.enable": "true", # enable metadata sync on the Glue catalog
    "hoodie.datasource.hive_sync.mode":"hms", # sync to Glue catalog
    # DynamoDB based locking mechanisms for concurrency control
    "hoodie.write.concurrency.mode":"optimistic_concurrency_control", #default is SINGLE_WRITER
    "hoodie.cleaner.policy.failed.writes":"LAZY", #Hudi will delete any files written by failed writes to re-claim space
    "hoodie.write.lock.provider":"org.apache.hudi.aws.transaction.lock.DynamoDBBasedLockProvider",
    "hoodie.write.lock.dynamodb.table":"myHudiLockTable",
    "hoodie.write.lock.dynamodb.partition_key":"tablename",
    "hoodie.write.lock.dynamodb.region": REGION,
    "hoodie.write.lock.dynamodb.endpoint_url": f"dynamodb.{REGION}.amazonaws.com"
}

##########################
# Job1 - inital load
##########################
# Reading initial contact CSV file with extra SCD columns
df_intial_csv= spark.read.schema(contact_schema)\
.format("csv")\
.options(header=False,delimiter=",")\
.load(f"s3://{S3_BUCKET_NAME}/blog/data/initial_contacts.csv")\
.withColumn("ts", lit(current_timestamp()).cast(TimestampType()))\
.withColumn("valid_from", lit(current_timestamp()).cast(TimestampType()))\
.withColumn("valid_to", lit("").cast(TimestampType()))\
.withColumn('iscurrent', lit(1).cast("int"))\
.withColumn("checksum",md5(concat(col("name"),col("email"),col("state"))))

#Write initial Hudi table to s3'
df_intial_csv.write.format("org.apache.hudi")\
              .option('hoodie.datasource.write.operation', 'insert')\
              .options(**hudiOptions)\
              .mode("overwrite")\
              .save(TABLE_LOCATION)

##########################
# Job2 - incremental load
##########################
## Read initial Hudi full table
initial_hudi_df = spark.read.format("org.apache.hudi").load(TABLE_LOCATION)
print('Records from the inital file :-', initial_hudi_df.count())

# Read incremental contact CSV file with extra SCD columns
delta_csv_df = spark.read.schema(contact_schema)\
.format("csv")\
.options(header=False,delimiter=",")\
.load(f"s3://{S3_BUCKET_NAME}/blog/data/update_contacts.csv")\
.withColumn("ts", lit(current_timestamp()).cast(TimestampType()))\
.withColumn("valid_from", lit(current_timestamp()).cast(TimestampType()))\
.withColumn("valid_to", lit("").cast(TimestampType()))\
.withColumn("checksum",md5(concat(col("name"),col("email"),col("state"))))\
.withColumn('iscurrent', lit(1).cast("int"))

print("Records from the incremental file :- ", delta_csv_df.count())  


## Find existing records to be expired
join_cond = [initial_hudi_df.checksum != delta_csv_df.checksum,
             initial_hudi_df.id == delta_csv_df.id,
             initial_hudi_df.iscurrent == 1]
contact_to_update_df = (initial_hudi_df.join(delta_csv_df, join_cond)
                      .select(initial_hudi_df.id, 
                              initial_hudi_df.name, 
                              initial_hudi_df.email, 
                              initial_hudi_df.state,
                              initial_hudi_df.ts,
                              initial_hudi_df.valid_from,
                              delta_csv_df.valid_from.alias('valid_to'),
                              initial_hudi_df.checksum
                              )
                      .withColumn('iscurrent', lit(0).cast("int"))
                      )

print("existing records to be expired :- ", contact_to_update_df.count())
merged_contact_df = delta_csv_df.unionByName(contact_to_update_df,allowMissingColumns=True)
print("Union with new records :- ", merged_contact_df.count())

# Upsert
merged_contact_df.write.format('org.apache.hudi')\
                    .option('hoodie.datasource.write.operation', 'upsert')\
                    .options(**hudiOptions) \
                    .mode('append')\
                    .save(TABLE_LOCATION)