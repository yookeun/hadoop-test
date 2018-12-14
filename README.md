## Hadoop 설치
> 설치버전 (stable) : hadoop-2.7.3
### 구성환경 
> 3 node 구성 

### 설치
tar xzf /home/ykkim/download/hadoop-2.7.3.tar.gz -C .


## /home/hadoop/etc/hadoop 안에 설정파일작업 

### core-site.xml
``` xml 
<property>
    <name>fs.dafault.name</name>
    <value>hdfs://hadoop1:9000</value>
</property>
```



### yarn-site.xml 

``` xml
<property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>1536</value>
</property>

<property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>1536</value>
</property>

<property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>128</value>
</property>

<property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
</property>
```


> cp mapred-site.xml.template mapred-site.xml

### mapred-site.xm
``` xml 
<property>
        <name>yarn.app.mapreduce.am.resource.mb</name>
        <value>512</value>
</property>

<property>
        <name>mapreduce.map.memory.mb</name>
        <value>256</value>
</property>

<property>
        <name>mapreduce.reduce.memory.mb</name>
        <value>256</value>
</property>
```
### slaves
```
hadoop2
hadoop3
```


### 다른 노드에 설치 

```
tar cfz hadoop.tar.gz *
scp hadoop.tar.gz hadoop2:/home/hadoop
scp hadoop.tar.gz hadoop3:/home/hadoop
```

