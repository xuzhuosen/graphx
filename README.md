# sparkgraphx

#### 使用 out目录下的jar包

#### 消费Kafka数据

```shell
spark-submit --master spark://namenode:7077 --deploy-mode client --executor-memory 512m --total-executor-cores 3 sparkgraphx.jar --consumer
```

#### 图分析

```shell
spark-submit --master spark://namenode:7077 --deploy-mode client --executor-memory 512m --total-executor-cores 3 sparkgraphx.jar --analyse
```

