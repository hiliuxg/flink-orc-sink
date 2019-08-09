# flink-orc-sink

通过Flink BucketingSink改造，启动checkpoint能保证端到端的exactly once

#### 落地文件有3个状态

1、当新记录来的时候，会根据目录路径，创建一个文件，该文件会以下划线"_"开头，in-process结尾，这时候，数据还在内存中，不会刷到文件系统，hive不可读

2、当checkpoint时候，会将内存的数据刷到文件系统，并将该目录下的in-process文件rename成以下划线"_"开头，in-appending结尾的文件，hive不可读

3、当收到checkpoint完成时的事件，会将该目录下in-appending文件最终转化成结果文件，这时，该文件可通过hive读取

#### 内存数据何时落到文件系统 ？

当checkpoint时，即触发snapshotState，会将内存数据落到文件系统，这时，文件是以in-appending结尾的，hive不可读，当接收到checkpoint完成事件时，即触发notifyCheckpointComplete方法时，会将in-appending文件最终转化成结果文件，这时，可通过hive读取

#### 出现异常时，如何容错 ？

出现异常，当前内存中的数据将会丢失，文件系统中in-process，in-appending状态的文件对于hive来说不可读，直接丢弃。
kafka source的offset将会重置到上一个checkpoint的offset重新消费，创建新文件

#### demo


```
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //enable checkpoint to guarantee exactly once
        env.enableCheckpointing(1 * 30 * 1000L);
        env.setParallelism(1);

        String basPath = "hdfs://xxxx:8020/user/hive/warehouse/temp.db/orc_test";
        String[] fields = {"x" ,"y"};
        TypeInformation[] typeInformations = {Types.INT, Types.INT};
        OrcSchema orcSchema = new OrcSchema(fields,typeInformations) ;

        //the orc sink
        RowOrcBucketingSink rowOrcBucketingSink = new RowOrcBucketingSink(basPath,orcSchema);

        //sink to hdfs
        env.socketTextStream("localhost",9000,"\n").map((MapFunction<String, Row>) value -> {
            String[] data = value.split(",");
            int x = Integer.parseInt(data[0]);
            int y = Integer.parseInt(data[1]);
            return Row.of(x,y);
        }).returns(TypeInformation.of(Row.class)).addSink(rowOrcBucketingSink);

        env.execute("orc sink test execute");
```

