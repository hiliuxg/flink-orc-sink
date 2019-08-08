package cn.hiliuxg.flink.sink.orc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.types.Row;
import org.junit.Test;


public class OrcSinkTest {


    @Test
    public void test() throws Exception {

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
    }


}
