package com.pandora.internalRating;

import com.pandora.internalRating.conf.Config;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.LocalDate;

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
                "code string,\n" +
                "name string,\n" +
                "type string, " +
                "rate string, " +
                "ratetype decimal(10, 0), " +
                "submittime string, " +
                "rankupdatedate string, " +
                "inputid string, " +
                "mktcode string, " +
                "mktcode01 decimal(10, 0), " +
                "watchrate string, " +
                "expectrate string, " +
                "isindraftdown decimal(10, 0), " +
                "afterdownrating string, " +
                "updatetimestamp timestamp(3), " +
                "tim decimal(10, 0) " +
                ") with (" +
                " 'connector' = 'oracle-x', " +
                " 'url' = '" + Config.sourceUrl + "', " +
                " 'table-name' = '" + Config.sourceTableName + "', " +
                " 'username' = '" + Config.sourceUserName + "', " +
                " 'password' = '" + Config.sourcePassword + "', " +
                " 'scan.polling-interval' = '30000', " +
                " 'scan.increment.column' = 'updatetimestamp', " +
                " 'scan.increment.column-type' = 'timestamp(3)' " +
                ")");


    }
}
