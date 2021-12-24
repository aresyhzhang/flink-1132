import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

/**
 * @ Author     ：aresyhzhang
 * @ Date       ：Created in 14:36 2021/12/24
 * @ Description：
 */

public class BaseMgdcTest {
    @Test
    public void  testData(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql( "create table test (\n" +
                "rowtime bigint,\n" +
                "pid string,\n" +
                "log_type string,\n" +
                "log_time string,\n" +
                "log_date string,\n" +
                "source string\n" +
                ")with(\n" +
                "'connector'='kafka',\n" +
                "'topic'='test_topic',\n" +
                "'properties.bootstrap.servers'='192.168.126.129:9092',\n" +
                "'format'='base-mgdc',\n" +
                "'base-mgdc.ignore-parse-errors'='true'\n" +
                ")" );
        tableEnv.executeSql( "select * from test" ).print();
    }
}