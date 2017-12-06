# Basic practise

## Get Data
- Upload file to HDFS
    * hadoop fs -put localfile /user/hadoop/hadoopfile
    * hadoop fs -put -f localfile1 localfile2 /user/hadoop/hadoopdir
    * hadoop fs -put -d localfile hdfs://nn.example.com/hadoop/hadoopfile
    * hadoop fs -put - hdfs://nn.example.com/hadoop/hadoopfile Reads the input from stdin.
    * hadoop fs -copyFromLocal file_name /data/path
    * http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html#put
- Copy data among clusters:
    * hadoop distcp hdfs://namenode/data hdfs://namenode/data
- Create directories to HDFS 
    * hadoop fs -mkdir /user/hadoop/dir1 /user/hadoop/dir2
    * hadoop fs -mkdir hdfs://nn1.example.com/user/hadoop/dir hdfs://nn2.example.com/user/hadoop/dir
    * http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html#mkdir
- Load data from RDS to HDFS
    * Connecting to a Database Server
        - sqoop import --connect jdbc:mysql://database.example.com/employees --username venkatesh --password-file ${user.home}/.password
        - sqoop import --driver com.microsoft.jdbc.sqlserver.SQLServerDriver --connect <connect-string>
        - http://sqoop.apache.org/docs/1.4.5/SqoopUserGuide.html#_literal_sqoop_import_literal
    * Selecting the Data to Import: --table employees
    * Free-form Query Imports
        - sqoop import --query 'SELECT a.*, b.* FROM a JOIN b on (a.id == b.id) WHERE $CONDITIONS' --split-by a.id --target-dir /user/foo/joinresults
    * sqoop import --connect jdbc:mysql:localhost:3306/logs --username root --password 12345 --table weblogs --target-dir /data/logs/weblogs -m 1
    * Importing Data Into Hive
        - --hive-import
        - http://sqoop.apache.org/docs/1.4.5/SqoopUserGuide.html#_importing_data_into_hive
- Export data to RDS from HDFS
    * sqoop export --connect jdbc:mysql:localhost:3306/logs --username root -password 123456 --table blogs_from_hdfs --export-dir '/data/weblogs/' -m 1 --fields-terminated-by '\t'
- Start a Flume agent
    * bin/flume-ng agent -n $agent_name -c conf -f conf/flume-conf.properties.template
- Setup Flume memory
    * https://flume.apache.org/FlumeUserGuide.html#memory-channel

## Transform Data
- Execute PIG script
    
    
    
## HBase
- Connect HBase: hbase shell
- Create table: create 'test', 'cf'
- List table information: list 'test'
- Get table description: describe 'test'
- Drop table(should disable table before drop it): drop 'test'
- Check if table exist: exists 'test'
- Insert data into table: put 'test', 'row1', 'cf:a', 'value1'
- Scan full table: scan 'test' 
- Get one row data: get 'test', 'row1'

## Hive
- Create simple table: CREATE TABLE table_name(field_name field_type)
- Create external table: CREATE EXTERNAL TABLE table_name(field_name field_type) location 'path'
- Create partitioned table: CREATE TABLE table_name(field_name field_type) PARTITIONED BY(sign_date STRING, age INT)
- Create Bucket table: CREATE TABLE table_name(field_name field_type) PARTITIONED BY(sign_date STRING) CLUSTERED BY(id) SORTED BY(age) INTO 5 BUCKETS ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
- Create empty table: CREATE table empty LIKE existing_table
- Drop table: DROP TABLE [IF EXIST] table_name [RESTRICT|CASCAD]
- Rename Table: ALTER Table table_name RNAME TO new_table_name
- Add or Delete partition: ALTER Table table_name ADD [IF NOT EXIST] partition_spec [LOCATION 'location1'] partition_spec [LOCATION 'location2']
- Add/Update Column: ALTER TAble table_name ADD|REPLACE COLUMNS (col_name data_type [COMMENT col_comment])
- Create View: CREATE VIEW [IF NOT EXIST] view_name [(col_name [COMMENT column_comment], ...)][Comment view_comment][TBLPROPERTIES (property_name = property_value, ...)]
- Drop View: DROP VIEW view_name
- Create function: CREATE TEMPORARY FUNCTION function_name as class_name
- Show database/table/function: show databases/tables/functions
- Load data to Hive table: LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE table_name [PARTITION (partcol1=val1, partcol2=val2 ...)]
- Insert query result into Hive table: INSERT OVERWRITE TABLE table_name [PARTITION (partcol=val1, partcol=val2...)] select_statement FROM from_statement
- Write query result to HDFS: INSERT OVERWRITE [LOCAL] DIRECTORY directory1 [ROW FORMAT row_format] [STORED AS file_name] SELECT ... FROM ...
- Insert data into Hive from SQL: INSERT INTO TABLE table_name [PARTITION (partcol1[=val1], partcol2[=val2] ...)] VALUES value_row [, values_row ...]
- Export data: EXPORT TABLE tablename [PARTITION (part_column="value"[, ...])] TO 'export_target_path'
- Import data: IMPORT IMPORT [[EXTERNAL] TABLE new_or_original_tablename [PARTITION (part_column="value"[, ...])]] FROM 'source_path' [LOCATION 'import_target_path']  
    
35.167.131.158 ip-10-0-0-170.us-west-2.compute.internal
35.160.83.16 ip-10-0-0-108.us-west-2.compute.internal
52.24.94.221 ip-10-0-0-220.us-west-2.compute.internal
52.27.152.12 ip-10-0-0-252.us-west-2.compute.internal
34.223.232.188 ip-10-0-0-33.us-west-2.compute.internal


https://docs.google.com/forms/d/e/1FAIpQLSf3-67pwgFnnyhqn1gHop7XTmFs8svuk_CtOa0bUomDexLOYg/viewform?usp=form_confirm