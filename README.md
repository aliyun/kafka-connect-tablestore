# Kafka Connect for TableStore
Kafka Sink Connector, 用于将 Kafka 中的数据写入 TableStore

首先部署zookeeper和kafka。然后通过本项目部署sink服务。    
通过mvn出包后，例如kafka-connect-tablestore-1.0-SNAPSHOT.jar，将这个包放在kafka项目的libs路径下。  
然后通过如下指令进行启动：  
```
nohup bin/connect-standalone.sh config/connect-standalone.properties config/connect-tablestore-sink-quickstart.properties > connector.out &
```
其中connect-standalone.properties为kafka包自带配置，不需要修改。  
connect-tablestore-sink-quickstart.properties为新增配置:
```
name=tablestore-sink
connector.class=TableStoreSinkConnector
tasks.max=1
topics=test
tablestore.endpoint=https://share-test.cn-hangzhou.ots.aliyuncs.com
tablestore.instance.name=share-test
auto.create=true
runtime.error.tolerance=all
runtime.error.mode=ignore
sts.endpoint=sts-vpc.cn-hangzhou.aliyuncs.com
client.time.out.ms=600000
region=cn-hangzhou
account.id=1396324980916621
role.name=Aliyuntest
```
服务账号的ak和sk需要配置在环境变量中  
ACCESS_ID=XXXX  
ACCESS_KEY=XXXX  
更多详情请参考表格存储官方文档https://help.aliyun.com/document_detail/309534.html