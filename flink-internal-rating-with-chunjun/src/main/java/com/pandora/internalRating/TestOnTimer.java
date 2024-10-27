package com.pandora.internalRating;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class TestOnTimer {
    private static Logger LOG = LoggerFactory.getLogger(TestOnTimer.class);
    private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        DataStreamSource<String> kafkaSourceDS = env.addSource(new FlinkKafkaConsumer<String>("test_topic", new SimpleStringSchema(), properties));
        SingleOutputStreamOperator<LocalDateTime> localDateTimeDS = kafkaSourceDS.map(value -> LocalDateTime.parse(value, dateTimeFormatter));

        SingleOutputStreamOperator<JSONObject> process = localDateTimeDS.keyBy(LocalDateTime::getYear)
                .process(new KeyedProcessFunction<Integer, LocalDateTime, JSONObject>() {

                    ValueState<LocalDateTime> maxSubmittimeValue;
                    ValueState<JSONObject> jsonObjectValue;
                    ValueState<Boolean> isTriggerTime;
                    long nextTriggerTime;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<LocalDateTime> valuestateDesctiptor = new ValueStateDescriptor<>("valuestate", LocalDateTime.class);
                        maxSubmittimeValue = getRuntimeContext().getState(valuestateDesctiptor);

                        ValueStateDescriptor<JSONObject> jsonValuestateDesctiptor = new ValueStateDescriptor<>("valuestate", JSONObject.class);
                        jsonObjectValue = getRuntimeContext().getState(jsonValuestateDesctiptor);

                        ValueStateDescriptor<Boolean> booleanValuestateDesctiptor = new ValueStateDescriptor<>("valuestate", Boolean.class);
                        isTriggerTime = getRuntimeContext().getState(booleanValuestateDesctiptor);
                    }

                    @Override
                    public void processElement(LocalDateTime value, Context ctx, Collector<JSONObject> out) throws Exception {
                        if (maxSubmittimeValue.value() == null) {
                            maxSubmittimeValue.update(value);
                        } else {
                            if (maxSubmittimeValue.value().isBefore(value)) {
                                maxSubmittimeValue.update(value);
                            }
                        }

                        JSONObject jsonObject = new JSONObject();

                        if (LocalDateTime.now().minusMinutes(2L).isAfter(value)) {
                            jsonObject.put("result", "此数据不在2分钟之内： " + value + "当前时间为：" + LocalDateTime.now());
                            jsonObjectValue.update(jsonObject);
                            out.collect(jsonObject);
                        } else {
                            jsonObject.put("result", "此数据在2分钟之内： " + value + "当前时间为：" + LocalDateTime.now());
                            jsonObjectValue.update(jsonObject);
                            out.collect(jsonObject);
                        }

                        if (isTriggerTime.value() == null) {
                            registerTimer(ctx);
                            isTriggerTime.update(true);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        System.out.println(">>>>触发定时器：" + LocalDateTime.ofEpochSecond(nextTriggerTime / 1000, 0, ZoneOffset.of("+08:00")) + "\n");
                        if (maxSubmittimeValue.value().isBefore(LocalDateTime.now().minusMinutes(2L))) {
                            JSONObject value = jsonObjectValue.value();
                            String result = value.getString("result");
                            String newResult;
                            newResult = "历史最大时间为" + result + "小于 当前时间： " + LocalDateTime.now() + "重置异常状态为0";
                            JSONObject newValue = new JSONObject();
                            newValue.put("result", newResult);
                            out.collect(newValue);
                        } else {
                            System.out.println(maxSubmittimeValue.value() + "在近两分钟内，所以存在有效变动数据，重置异常状态为1" + "当前时间:" + LocalDateTime.now());
                        }
                        registerTimer(ctx);
                    }

                    private void registerTimer(Context ctx) {
                        LocalDateTime currentDateTime = LocalDateTime.ofEpochSecond(ctx.timerService().currentProcessingTime() / 1000, 0, ZoneOffset.of("+08:00"));
                        LocalDateTime nextTriggerDateTime = currentDateTime.withSecond(0).plusMinutes(1L);

                        nextTriggerTime = nextTriggerDateTime.toEpochSecond(ZoneOffset.of("+08:00")) * 1000L;

                        try {
                            //注册定时器
                            ctx.timerService().registerProcessingTimeTimer(nextTriggerTime);
                            System.out.println("下次定期器触发" + LocalDateTime.ofEpochSecond(nextTriggerTime / 1000, 0, ZoneOffset.of("+08:00")) + "\n");
                        } catch (Exception e) {
                            LOG.error("触发下一次定时器异常！！！");
                        }
                    }
                });
        process.print();
        env.execute();
    }
}
