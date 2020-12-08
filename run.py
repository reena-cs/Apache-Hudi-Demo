from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import datetime

# ### s3 path

s3_path = "s3://<bucket-name>/hudi_output/"
data_path = "s3://<bucket-name>/hudi_input/Hudi_input_10K.json"

tableName = "MainTable"
derivedTable = "derivedTable"
tradnl_table = "traditional"

basePath = s3_path + tableName
basePathDer = s3_path + derivedTable
traditional_path = s3_path + tradnl_table


# ## 1. Mocked data lake setup

# ### a. read from data source

df = spark.read.json(path=data_path)

# ### b. write to data lake

df.write.partitionBy("partition_date").format("parquet").mode("overwrite").save(traditional_path)


# ## 2. Query sample data and show stats 

print(f"count :{df.count()} ")
print(f"distinct count :{df.distinct().count()} ")
print(f"columns : {df.columns}")
print(f'distinct factors col value:{df.select("factor").distinct().show()}')
print(df.show(2, truncate=False))


# ### read data from old data lake

def extract_date(data_str):
    return data_str.split("/")[-2].split("=")[1]

conversion_df = spark.read.parquet(traditional_path+"*/*").withColumn("partition_date", input_file_name())
conversion_fun = udf(extract_date, StringType())
conversion_df = conversion_df.withColumn("partition_date", conversion_fun(conversion_df['partition_date']))
conversion_df.show(1, truncate=False)
# alternatively you can read from athena as-well

# ## 3. Run one-time conversion to Hudi format

# ### a.hudi configs


hudi_options = {
  'hoodie.table.name': tableName,
  'hoodie.datasource.write.recordkey.field': 'cookie,partition_date',
  'hoodie.datasource.write.keygenerator.class':'org.apache.hudi.keygen.ComplexKeyGenerator',
  'hoodie.datasource.write.partitionpath.field': 'partition_date',
  'hoodie.datasource.write.table.name': tableName,
  'hoodie.datasource.write.operation': 'insert, delete, upsert',
  'hoodie.datasource.write.precombine.field': 'partition_year',
  'hoodie.upsert.shuffle.parallelism': 2, 
  'hoodie.insert.shuffle.parallelism': 2,
}


# ### b. hudi table creation

conversion_df.write.format("org.apache.hudi").options(**hudi_options).mode("overwrite").save(basePath)


# ### c. simple stats on the newly created table

dff = spark.read.format("hudi").load(basePath + "*/*")
print(f"records in data lake : {dff.count()}")
print(f"column names : {dff.columns}")


# ## 4. Show output in data lake

# ### hudi write - derived table


df_derived = conversion_df.withColumn("transform_col",lit(conversion_df.cookie+conversion_df.country_code))

hudi_options_derived = {
  'hoodie.table.name': derivedTable,
  'hoodie.datasource.write.recordkey.field': 'cookie',
  'hoodie.datasource.write.partitionpath.field': 'partition_date',
  'hoodie.datasource.write.table.name': derivedTable,
  'hoodie.datasource.write.operation': 'insert',
  'hoodie.datasource.write.precombine.field': 'partition_year',
  'hoodie.upsert.shuffle.parallelism': 2, 
  'hoodie.insert.shuffle.parallelism': 2,
}

df_derived.write.format("org.apache.hudi").options(**hudi_options_derived).mode("overwrite").save(basePathDer)

dff_derived = spark.read.format("hudi").load(basePathDer + "*/*")
print(f"records in data lake : {dff_derived.count()}")
print(f"column names : {dff_derived.columns}")


# ## 5. Run DELETE on data lake

# ### a. count before delete


prev_count = spark.read.format("hudi").load(basePath + "*/*").count()
print(f"count before delete operation: {prev_count}")

# ### b. delete few records

# config change -- operation
hudi_options['hoodie.datasource.write.operation'] = 'delete'
to_delete = conversion_df.limit(10)

to_delete.write.format("hudi").options(**hudi_options).mode("append").save(basePath)


# ### c. count after delete

post_count= spark.read.format("hudi").load(basePath + "*/*").count()
print(f"count after delete operation: {post_count}")


# ## 6. Query sample data 

# ## 7. UPSERT DATA on data lake
# 

# ### a. records to update


update_df = conversion_df.limit(5)
update_dff = update_df.withColumn("factor",lit("2"))


# ### b. update operation

hudi_options['hoodie.datasource.write.operation'] = 'upsert'

update_dff.write.format("org.apache.hudi").options(**hudi_options).mode("append").save(basePath)


# ### c. reload data from hudi and verify the update


# reload -- updated value should be reflected
spark.read.format("hudi").load(basePath + "*/*").select("factor").distinct().show()


# ## 8. Query sample data 

# ## 9. incremental pull


loaded_df = spark.read.format("hudi").load(basePath + "*/*")
commit_inorder = loaded_df.select("_hoodie_commit_time").distinct().sort(col("_hoodie_commit_time").asc())
commits = list(map(lambda row: row[0], commit_inorder.limit(50).collect()))

print(f"commits : {commits}")

beginTime = commits[len(commits) - 2]
incremental_read_options = {
  'hoodie.datasource.query.type': 'incremental',
  'hoodie.datasource.read.begin.instanttime':beginTime ,
}

IncrementalDF = spark.read.format("hudi").options(**incremental_read_options).load(basePath)

to_update_df = IncrementalDF.select(['cookie','country_code','factor','partition_date','partition_month','_hoodie_partition_path'])
to_update_df.show()


# ##  10. ROLL BACK on data lake --refer ReadMe
