# Apache-Hudi-Demo
A small example which showcases Hudi Insert, Update and delete.


# sharting a spark shell
pyspark   --packages org.apache.hudi:hudi-spark-bundle_2.11:0.6.0,org.apache.spark:spark-avro_2.11:2.4.6   --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf "spark.sql.hive.convertMetastoreParquet=false" 

# Table creation in Athena

CREATE EXTERNAL TABLE `MainTable`(
  `_hoodie_commit_time` string, 
  `_hoodie_commit_seqno` string, 
  `_hoodie_record_key` string, 
  `_hoodie_partition_path` string, 
  `_hoodie_file_name` string, 
  `cookie` string, 
  `country_code` string, 
  `factor` string,
  `partition_month` string, 
  `partition_year` string
   )
   
PARTITIONED BY ( 
  `partition_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hudi.hadoop.HoodieParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://bucket-name/hudi_output/Maintable'
  
  
  ## add partitions
 
 ALTER TABLE cookietable ADD
 PARTITION (partition_date='20201001') 
 LOCATION 's3://bucket-name/hudi_output/Maintable/20201001'
 

 ALTER TABLE cookietable ADD
 PARTITION (partition_date='20201002') 
 LOCATION 's3://bucket-name/hudi_output/Maintable/20201002'
 
  ALTER TABLE cookietable ADD
 PARTITION (partition_date='20201003') 
 LOCATION 's3://bucket-name/hudi_output/Maintable/20201003'

select count(distinct(factor)) from "hudi"."Maintable"


## commit rollback - use AWS EMR CLI
/usr/lib/hudi/cli/bin/hudi-cli.sh

connect --path s3://bucket-name/hudi_output/Maintable
  
desc 

help

commits show --sortBy "CommitTime" --desc true --limit 10

commit rollback --commit ##commit_no##
