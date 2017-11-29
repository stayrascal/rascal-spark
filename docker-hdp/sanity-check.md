Login to your datanode, create a Hadoop home directory for "admin", and put some sample logfiles into it
```
localhost:randy$ docker exec -it compose_master0.dev_1 bash
[root@dn0 /]# su hdfs
bash-4.2$ hdfs dfs -mkdir /user/admin
bash-4.2$ hdfs dfs -chown admin /user/admin
```

Run the following Hive queries to create raw and derived wordcount tables from the logfiles:
```
[root@dn0 /]# hive
hive> create table loglines (line string);
hive> load data local inpath '/var/log/ambari-agent/' overwrite into loglines;
hive> create table words as
select word, count(*) as count
from (
  select
    explode(split(line, " ")) as word
  from loglines
) a
group by word;
```
