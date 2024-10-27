package com.pandora.internalRating.conf;

import java.util.Properties;

public class Config {
    public static String env;
    public static String sourceUrl;
    public static String sourceTableName;
    public static String sourceUserName;
    public static String sourcePassword;
    public static String taimetahubUrl;
    public static String taimetahubUserName;
    public static String taimetahubPassword;
    public static String sinkDriverName;
    public static String sinkUrl;
    public static String sinkTableNameBond;
    public static String sinkTableNameIssuer;
    public static String sinkUserName;
    public static String sinkPassword;

    static {
        Properties properties = new Properties();
        try {
            properties.load(Config.class.getClassLoader().getResourceAsStream(env + "/config.properties"));
            sourceUrl = properties.getProperty("source.orcale.url");
            sourceTableName = properties.getProperty("source.orcale.tablename");
            sourceUserName = properties.getProperty("source.orcale.username");
            sourcePassword = properties.getProperty("source.orcale.password");

            taimetahubUrl = properties.getProperty("taimetahubUrl");
            taimetahubUserName = properties.getProperty("taimetahubUserName");
            taimetahubPassword = properties.getProperty("taimetahubPassword");
            sinkDriverName = properties.getProperty("sink.mysql.driverName");
            sinkUrl = properties.getProperty("sink.mysql.url");
            sinkTableNameBond = properties.getProperty("sink.mysql.tablename.bond");
            sinkTableNameIssuer = properties.getProperty("sink.mysqlj.tablename.issuer");
            sinkUserName = properties.getProperty("sink.mysql.username");
            sinkPassword = properties.getProperty("sink.mysql.password");
        } catch (Exception e) {
            System.out.println("init properties failure: " + e.getMessage());
        }
    }
}
