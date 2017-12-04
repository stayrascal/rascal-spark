# Basic practise

## Get Data
- Upload file to HDFS
    * hadoop fs -put localfile /user/hadoop/hadoopfile
    * hadoop fs -put -f localfile1 localfile2 /user/hadoop/hadoopdir
    * hadoop fs -put -d localfile hdfs://nn.example.com/hadoop/hadoopfile
    * hadoop fs -put - hdfs://nn.example.com/hadoop/hadoopfile Reads the input from stdin.
    * http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/FileSystemShell.html#put
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
    * Importing Data Into Hive
        - --hive-import
        - http://sqoop.apache.org/docs/1.4.5/SqoopUserGuide.html#_importing_data_into_hive
- Start a Flume agent
    * bin/flume-ng agent -n $agent_name -c conf -f conf/flume-conf.properties.template
- Setup Flume memory
    * https://flume.apache.org/FlumeUserGuide.html#memory-channel

## Transform Data
- Execute PIG script
    
    
    
35.167.131.158 ip-10-0-0-170.us-west-2.compute.internal
35.160.83.16 ip-10-0-0-108.us-west-2.compute.internal
52.24.94.221 ip-10-0-0-220.us-west-2.compute.internal
52.27.152.12 ip-10-0-0-252.us-west-2.compute.internal
34.223.232.188 ip-10-0-0-33.us-west-2.compute.internal