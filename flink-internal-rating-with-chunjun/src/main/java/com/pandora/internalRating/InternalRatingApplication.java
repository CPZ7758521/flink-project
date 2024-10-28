package com.pandora.internalRating;

import com.alibaba.fastjson.JSONObject;
import com.pandora.internalRating.conf.Config;
import com.pandora.internalRating.utils.Utils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashSet;

/**
 * 上一个版本中，改成了状态中不能只保存一个最终结果，还是要保存value的每一条，但是也不是全保留在状态中
 *
 * if state 中的min_submittime < currentdate - 30
 * then 新来的数据的submintime < min_submittine则什么都不做
 *      新来的数据的submittime > min_submittime扔到状态中，找到currentdate - 30最近的一个min_submittime，小于这个min_subittime的数据扔掉
 */
public class InternalRatingApplication {
    private static String SUBMITTIME_LOCALDATE_PATTERN = "yyyyMMdd";

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        /**
         * 首先利用chujun的oracle-x增量读取数据
         */

        tableEnv.executeSql("create table bond_company_history\n" +
                "CODE string,\n" +
                "NAME string,\n" +
                "TYPE string, " +
                "RATE string, " +
                "RATETYPE decimal(10, 0), " +
                "SUBMITTIME string, " +
                "RANKUPDATEDATE string, " +
                "INPUTID string, " +
                "MKTCODE string, " +
                "MKTCODE01 decimal(10, 0), " +
                "WATCHRATE string, " +
                "EXPECTRATE string, " +
                "ISINDRAFTDOWN decimal(10, 0), " +
                "AFTERDOWNRATING string, " +
                "UPDATETIMESTAMP timestamp(3), " +
                "TIM decimal(10, 0) " +
                ") with (" +
                " 'connector' = 'oracle-x', " +
                " 'url' = '" + Config.sourceUrl + "', " +
                " 'table-name' = '" + Config.sourceTableName + "', " +
                " 'username' = '" + Config.sourceUserName + "', " +
                " 'password' = '" + Config.sourcePassword + "', " +
                " 'scan.polling-interval' = '30000', " +
                " 'scan.increment.column' = 'UPDATETIMESTAMP', " +
                " 'scan.increment.column-type' = 'timestamp(3)' " +
                ")");

        Table table = tableEnv.sqlQuery("select " +
                "CODE ,\n" +
                "NAME ,\n" +
                "TYPE , " +
                "RATE , " +
                "RATETYPE , " +
                "SUBMITTIME , " +
                "RANKUPDATEDATE , " +
                "INPUTID , " +
                "MKTCODE , " +
                "MKTCODE01 , " +
                "WATCHRATE , " +
                "EXPECTRATE , " +
                "ISINDRAFTDOWN , " +
                "AFTERDOWNRATING , " +
                "UPDATETIMESTAMP , " +
                "TIM  " +
                "from sirm_bond_company_history" +
                "");

        /**
         * 将table api 转为 datastream，转换后的数据对应上字段名，并转为 Jsonobject
         */
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
        SingleOutputStreamOperator<JSONObject> rowJsonObjectDS = rowDataStream.map(new MapFunction<Row, JSONObject>() {
            @Override
            public JSONObject map(Row row) throws Exception {
                RowKind rowKind = row.getKind();
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("rowkind", rowKind);

                String[] fields = {
                        "CODE",
                        "NAME",
                        "TYPE",
                        "RATE",
                        "RATETYPE",
                        "SUBMITTIME",
                        "RANKUPDATEDATE",
                        "INPUTID",
                        "MKTCODE",
                        "MKTCODE01",
                        "WATCHRATE",
                        "EXPECTRATE",
                        "ISINDRAFTDOWN",
                        "AFTERDOWNRATING",
                        "UPDATETIMESTAMP",
                        "TIM"
                };

                for (int i = 0; i < row.getArity(); i++) {
                    Object field = row.getField(i);
                    jsonObject.put(fields[i], field == null ? null : field.toString());
                }

                return jsonObject;
            }
        });

        /**
         * 定义侧输出流的tag，侧输出流中输出类型为1或2的数据，进行分流
         */

        OutputTag<JSONObject> bondType_12 = new OutputTag<JSONObject>("bondType_12"){};

        /**
         * 按照
         * 债项 初评（1、2）
         * 债项 正式评级（3、4、5）
         * 主体 初评（1、2）
         * 主体 正式评级（3、4、5）
         * 结合侧输出流分为4条流
         *
         * 侧输出流要在process之后立刻获取，再做后续处理，在有侧输出流的process后，不可以继续 用点 .算子 继续处理数据，
         * 侧输出流会被后续算子抹除，导致拿不到数据。
         */

        rowJsonObjectDS
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        String bond_code = Utils.convertBondCode(value.getString("MKTCODE01"), value.getString("CODE"));
                        value.put("bond_code", bond_code);
                        return value;
                    }
                })
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String mktcode01 = value.getString("MKTCODE01");
                        String code = value.getString("CODE");
                        String type = value.getString("TYPE");
                        String[] mktcode01Arr = {"1", "2", "4"};
                        HashSet<String> mktcode01Set = new HashSet<>(Arrays.asList(mktcode01Arr));
                        return code != null && ((mktcode01Set.contains(mktcode01) && "债项".equals(type)) || "主体".equals(type));
                    }
                })
                .keyBy(value -> value.getString("TYPE"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    MapState<String, Integer> rateCodeMap;
                    Connection conn;
                    PreparedStatement queryPreparedStatement;
                    ResultSet resultSet;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, Integer> rateCodeMapStateDescriptor = new MapStateDescriptor<>("rateCodeMapStateDescriptor", String.class, Integer.class);
                        rateCodeMap = getRuntimeContext().getMapState(rateCodeMapStateDescriptor);

                        /**
                         * join 维表数据量较少直接缓存到状态中
                         */

                        Class.forName("com.mysql.cj.jdbc.Driver");
                        conn = DriverManager.getConnection(Config.taimetahubUrl, Config.taimetahubUserName, Config.taimetahubPassword);
                        String querySqlStr = "select code_id, code_name, tag_en from dimemsiondb.tag_code_table where tag_en in ('bond_rating_code', 'issuer_rating_code')";

                        queryPreparedStatement = conn.prepareStatement(querySqlStr);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {

                    }
                })
    }
}
