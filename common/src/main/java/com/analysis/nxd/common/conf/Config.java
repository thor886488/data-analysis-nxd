package com.analysis.nxd.common.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ResourceBundle;

/**
 * @author ：kequan
 * @date ：Created in 2020/5/27 11:37
 * @description：配置类
 * @modified By：
 */
public class Config {
    private static final Logger logger = LoggerFactory.getLogger(Config.class);
    public static String ENV_ACTIVE;
    public static String DORIS_DRIVER;
    public static String DORIS_FENODES;
    public static String DORIS_URL;
    public static String DORIS_USER;
    public static String DORIS_PASSWORD;
    public static String MYSQL_URL;
    public static String MYSQL_USER;
    public static String MYSQL_PASSWORD;
    public static String HDFS_URL;
    public static String BROKER_NAME;
    public static String SLACK_BOT_TOKEN;
    public static String SLACK_CHANNEL_FH4;
    public static String SLACK_CHANNEL_BM;
    public static String SLACK_CHANNEL_YFT;
    public static String SLACK_CHANNEL_2HZN;
    public static String SLACK_CHANNEL_BM2;
    public static String SLACK_CHANNEL_1HZ;
    public static String SLACK_CHANNEL_FH3;

    static {
        //指定要读取的配置文件
        ResourceBundle bundle = ResourceBundle.getBundle("common");
        //获取配置文件里面内容
        ENV_ACTIVE = bundle.getString("env.active").trim();
        DORIS_DRIVER = bundle.getString("doris.driver").trim();
        DORIS_FENODES = bundle.getString("doris.fenodes").trim();
        DORIS_URL = bundle.getString("doris.url").trim();
        DORIS_USER = bundle.getString("doris.user").trim();
        DORIS_PASSWORD = bundle.getString("doris.password").trim();

        MYSQL_URL = bundle.getString("mysql.url").trim();
        MYSQL_USER = bundle.getString("mysql.user").trim();
        MYSQL_PASSWORD = bundle.getString("mysql.password").trim();

        HDFS_URL = bundle.getString("hdfs.url").trim();
        BROKER_NAME = bundle.getString("broker.name").trim();
        SLACK_BOT_TOKEN = bundle.getString("slack.bot.token").trim().replace("===", "").replace("XXX", "");

        SLACK_CHANNEL_FH4 = bundle.getString("slack.bot.channel.fh4").trim();
        SLACK_CHANNEL_BM = bundle.getString("slack.bot.channel.bm").trim();
        SLACK_CHANNEL_YFT = bundle.getString("slack.bot.channel.yft").trim();
        SLACK_CHANNEL_2HZN = bundle.getString("slack.bot.channel.2hzn").trim();
        SLACK_CHANNEL_BM2 = bundle.getString("slack.bot.channel.bm2").trim();
        SLACK_CHANNEL_1HZ = bundle.getString("slack.bot.channel.1hz").trim();
        SLACK_CHANNEL_FH3 = bundle.getString("slack.bot.channel.fh3").trim();

        logger.info("----------  start  ------------- ");
        logger.info("----------  运行环境 : {}------------- ", ENV_ACTIVE);
    }
}
