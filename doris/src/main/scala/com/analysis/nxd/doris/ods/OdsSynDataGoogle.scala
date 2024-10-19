package com.analysis.nxd.doris.ods

import com.analysis.nxd.common.utils.{DateUtils, GoogleSheetsUtils, JdbcUtils}

import java.sql.Connection

/**
 * 一些不经常变化的配置
 */
object OdsSynDataGoogle {
  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, dorisConn: Connection, mysqlConn: Connection): Unit = {
    JdbcUtils.execute(dorisConn, "use doris_thirdly", "use doris_thirdly")
    //FH4 ag 配置云文档
    val listAgConf = GoogleSheetsUtils.readGoogleSheet("1cnpNmQaj1MeeqT1jArWU5VNklABRYeFkxrHp59C6bog", "FH4-AG游戏分类!A2:B");
    JdbcUtils.executeList(dorisConn, mysqlConn, "ods_fh4_ag_conf", listAgConf)

    //三方字段-帐变公式
    val listTranThird = GoogleSheetsUtils.readGoogleSheet("1gY_bhzMyyRF4sfWUJ_uIVpyW2nqYbHLtY90UZ9NoveE", "工作表1!A2:F", null, "site_code,type_code,type_name,paren_type_code,paren_type_name,thirdly_code", null)
    JdbcUtils.executeList(dorisConn, mysqlConn, "dwd_thirdly_transaction_types", listTranThird)

    //  三方类型配置表
    val list3rdKindnameConf = GoogleSheetsUtils.readGoogleSheet("1janmcX_IjRh9togF85126sI9kfUEN8oxmVlsxCUEEP0", "工作表1!A2:D", null, "site_code,thirdly_code,game_code,kind_name", null)
    JdbcUtils.executeList(dorisConn, mysqlConn, "app_thirdly_kind_conf", list3rdKindnameConf)


    // 三方 gp1.5 计算公式表
    val listGp15Thirdly = GoogleSheetsUtils.readGoogleSheet("1gY_bhzMyyRF4sfWUJ_uIVpyW2nqYbHLtY90UZ9NoveE", "工作表1!A2:C", null, "site_code,type_code,type_name", null);
    JdbcUtils.executeList(dorisConn, mysqlConn, "dwd_third_transaction_types_gp1_5", listGp15Thirdly)
    val sql_dwd_third_transaction_types_gp1_5_1hz0 =
      """
        |insert into dwd_third_transaction_types_gp1_5
        |SELECT  '1HZ0' site_code,type_code,type_name,updated_at from  dwd_third_transaction_types_gp1_5 where site_code ='1HZ'
        |""".stripMargin
    JdbcUtils.execute(dorisConn, "sql_dwd_third_transaction_types_gp1_5_1hz0", sql_dwd_third_transaction_types_gp1_5_1hz0)

    // 三方 gp2 计算公式表
    val listGp2Thirdly = GoogleSheetsUtils.readGoogleSheet("1ae-uu113sZP7Qz7NOG_4zoef81VUFowHULXECskoBPo", "工作表1!A2:C", null, "site_code,type_code,type_name", null);
    JdbcUtils.executeList(dorisConn, mysqlConn, "dwd_third_transaction_types_gp2", listGp2Thirdly)
    val sql_dwd_third_transaction_types_gp2 =
      """
        |insert into dwd_third_transaction_types_gp2
        |SELECT site_code,type_code,type_name,updated_at from  dwd_third_transaction_types_gp1_5
        |""".stripMargin
    val sql_dwd_third_transaction_types_gp2_1hz0 =
      """
        |insert into dwd_third_transaction_types_gp2
        |SELECT  '1HZ0' site_code,type_code,type_name,updated_at from  dwd_third_transaction_types_gp2 where site_code ='1HZ'
        |""".stripMargin
    JdbcUtils.execute(dorisConn, "sql_dwd_third_transaction_types_gp2", sql_dwd_third_transaction_types_gp2)
    JdbcUtils.execute(dorisConn, "sql_dwd_third_transaction_types_gp2_1hz0", sql_dwd_third_transaction_types_gp2_1hz0)

    //  GCP表導入_BM_3rd
    // val listGCP3rdBMmonth = GoogleSheetsUtils.readGoogleSheet("1n5jH3ZXXnPxtPZh8tzrwDabHw7Or0KGtwh2qsu6lDLc", "风险用户名单!A1:L", null, "USER_ID,USER_NAME,TRUNC_YMD__MM__,SUM_BET_AMT_,SUM_PRIZE_AMT_,SUM_COMMISSION_AMT_,SUM_REBATE_AMT_,SUM_PROMOTION_AMT_,SUM_INCENT_AMT_,SUM_COMMISSION_AMT__1,SUM_RECHARGE_AMT_,SUM_WITHDRAW_AMT_", null);
    // JdbcUtils.executeList(dorisConn, mysqlConn, "obi_bm_third_month", listGCP3rdBMmonth)

    JdbcUtils.execute(dorisConn, "use doris_dt", "use doris_dt")
    // 彩种统一名称配置
    val listLotteryConf = GoogleSheetsUtils.readGoogleSheet("1qM7dhF4Rm2rzpZj_0FUxP0iDB4wDnswYy1ftt-9MQYY", "EC采种名称用!A2:U", null, "site_code,lottery_code,series_code,series_name,lottery_name,lottery_name_cn,lottery_type,lottery_type_name,series_type,is_self,is_high,link,monitor,bm_type_name,bm_type_code,bm_kind_name,bm_kind_code,lottery_name_nac,link_1,nac_code,update_time", null);
    JdbcUtils.executeList(dorisConn, mysqlConn, "ods_lottery_conf", listLotteryConf)
    val sql_ods_lottery_conf_1hz0 =
      """
        |insert  into  ods_lottery_conf
        |select  '1HZ0' site_code,lottery_code,series_code,series_name,lottery_name,lottery_name_cn,lottery_type,lottery_type_name,series_type,is_self,is_high,REPLACE(link,'1HZ','1HZ0') link,monitor,bm_type_name,bm_type_code,bm_kind_name,bm_kind_code,lottery_name_nac,REPLACE(link,'1HZ','1HZ0') link_1,nac_code,update_time,updated_at  from  ods_lottery_conf  where  site_code='1HZ'
        |""".stripMargin
    JdbcUtils.execute(dorisConn, "sql_ods_lottery_conf_1hz0", sql_ods_lottery_conf_1hz0)

    val sql_ods_lottery_conf_un_know =
      """
        |insert  into  ods_lottery_conf
        |select t.site_code
        |,t.lottery_code
        |,t.series_code
        |,t.series_name
        |,t.lottery_name
        |,ifnull(c.lottery_name_cn,'未配置')  lottery_name_cn
        |,ifnull(c.lottery_type,'未配置')   lottery_type
        |,ifnull(c.lottery_type_name,'未配置')   lottery_type_name
        |,ifnull(c.series_type,'未配置')   series_type
        |,ifnull(c.is_self,'未配置')   is_self
        |,ifnull(c.is_high,'未配置')   is_high
        |,concat(t.site_code,'_',t.lottery_code) link
        |,ifnull(c.monitor,'未配置')  monitor
        |,ifnull(c.bm_type_name,'未配置') bm_type_name
        |,ifnull(c.bm_type_code,'未配置') bm_type_code
        |,ifnull(c.bm_kind_name,'未配置') bm_kind_name
        |,ifnull(c.bm_kind_code,'未配置') bm_kind_code
        |,ifnull(c.lottery_name_nac,'未配置') lottery_name_nac
        |,ifnull(c.link_1,'未配置') link_1
        |,ifnull(c.nac_code,'未配置') nac_code
        |,now() update_time
        |,now() updated_at
        |from
        |(
        |select  site_code,lottery_code,max(lottery_name) lottery_name ,max(series_code) series_code ,max(series_name) series_name from
        | app_day_lottery_kpi
        |where
        | data_date>=date_sub(now(),30) and   site_code  is  not null and  lottery_code is not  null  and  lottery_code<>'0'
        |group  by
        |site_code,lottery_code
        |) t
        |left  join   ods_lottery_conf  c on  t.site_code= c.site_code  and    t.lottery_code= c.lottery_code
        |""".stripMargin
    JdbcUtils.execute(dorisConn, "sql_ods_lottery_conf_un_know", sql_ods_lottery_conf_un_know)

    // 代理统一表
    val listAgentConf = GoogleSheetsUtils.readGoogleSheet("1c8ffyNAweFbyA0wdSnotyac0Jklu_PWqK_MOMYKw8XY", "各站点总代名单!A2:H", null, "data_month,site_code,agent_0,agent_group,dep,supervisor,supervision,is_self", null);
    JdbcUtils.executeList(dorisConn, mysqlConn, "ods_agent_month_conf", listAgentConf)

    val endMonth = DateUtils.getFirstDayOfMonth(DateUtils.getSysFullDate)
    val startMonth = DateUtils.addMonth(endMonth, -60) + "-01"

    for (months <- 0 to 60) {
      val runMonth = DateUtils.addMonth(startMonth, months) + "-01"
      val nextMonth = DateUtils.addMonth(runMonth, 1) + "-01"
      val sql_dwd_agent_month_conf =
        s"""
           |insert  into  dwd_agent_month_conf
           |select
           |u.data_month
           |,u.site_code
           |,u.username
           |,ifnull(a.agent_group,'未配置') agent_group
           |,ifnull(a.dep,'未配置')  dep
           |,ifnull(a.supervisor,'未配置') supervisor
           |,ifnull(a.supervision,'未配置') supervision
           |,ifnull(a.is_self,'未配置')   is_self
           |,now() updated_at
           |from
           |(
           |SELECT  distinct
           | left('$runMonth',7)  data_month
           |,CASE site_code
           |    WHEN '1HZ0' THEN  '1HZ'
           |    ELSE site_code
           |END site_code
           |,username from dwd_users where   user_level=0 and  is_agent=1
           |and site_code<>'2HZ'
           |and created_at <= concat('$nextMonth',' 00:00:00')
           |) u
           |left  join  ods_agent_month_conf  a on u.site_code=a.site_code  and   lower(u.username)=lower(a.agent_0)  and   u.data_month=a.data_month
           |""".stripMargin

      val sql_dwd_agent_month_conf_all =
        s"""
           |insert  into  dwd_agent_month_conf
           |SELECT  left('$runMonth',7),site_code,agent_0,agent_group,dep,supervisor,supervision,is_self,updated_at from
           |(
           |SELECT *,ROW_NUMBER() OVER(PARTITION BY site_code,agent_0 ORDER BY  date(concat(data_month,'-01')) desc )  rank_time  from  dwd_agent_month_conf
           |where  date(concat(data_month,'-01')) <='$runMonth'
           |and  agent_group<>'未配置'
           |) t  where  rank_time=1
           |""".stripMargin

      if (DateUtils.compareDate(runMonth, "2021-01-01") >= 0) {
        JdbcUtils.execute(dorisConn, "sql_dwd_agent_month_conf", sql_dwd_agent_month_conf)
        JdbcUtils.execute(dorisConn, "sql_dwd_agent_month_conf_all", sql_dwd_agent_month_conf_all)
      }

    }

    val sql_dwd_agent_month_conf_1hz0 =
      """
        |insert  into  dwd_agent_month_conf
        |select  data_month,'1HZ0' site_code,agent_0,agent_group,dep,supervisor,supervision,is_self,updated_at from  dwd_agent_month_conf  where  site_code='1HZ'
        |""".stripMargin
    JdbcUtils.execute(dorisConn, "sql_dwd_agent_month_conf_1hz0", sql_dwd_agent_month_conf_1hz0)


    // 账变表
    val listTransactionTypes = GoogleSheetsUtils.readGoogleSheet("1YkdkFqx_SWjZK8IywDIo9GFH7LhoikZvtusfUJRE8uY", "各站新增帐变列表!A2:E", null, "site_code,type_code,type_name,pm_available,description", null);
    JdbcUtils.executeList(dorisConn, mysqlConn, "ods_transaction_types", listTransactionTypes)

    val listTran = GoogleSheetsUtils.readGoogleSheet("1mwVgYd0-f7KHHX6c_wmWLh69jZNCSiNzGxTWvk0AQTY", "工作表1!A2:E", null, "site_code,type_code,type_name,paren_type_code,paren_type_name", null)
    JdbcUtils.executeList(dorisConn, mysqlConn, "dwd_transaction_types_parent", listTran)

    // 风险用户表
    // val listUserRisk = GoogleSheetsUtils.readGoogleSheet("1Xq0XdwI84Di1VwahWLRaxgUALeQFM129iPCcSDyQZhU", "Fake Player Consulting!A2:H", null, "Date,Backend/Client,User Name", null);
    // JdbcUtils.executeList(dorisConn, mysqlConn, "ods_user_risk", listUserRisk)

    // 代理总费用 计算公式表
    val listAgentCost = GoogleSheetsUtils.readGoogleSheet("1coHDXMIqpvN0b0Hn8hSr6P0mSWzlMWJuK1cLw49nq5E", "工作表1!A2:C", null, "site_code,type_code,type_name", null);
    JdbcUtils.executeList(dorisConn, mysqlConn, "dwd_transaction_types_agent_cost", listAgentCost)

    val sql_dwd_transaction_types_agent_cost_1hz0 =
      """
        |insert into dwd_transaction_types_agent_cost
        |SELECT  '1HZ0' site_code,type_code,type_name,updated_at from  dwd_transaction_types_agent_cost where site_code ='1HZ'
        |""".stripMargin
    JdbcUtils.execute(dorisConn, "sql_dwd_transaction_types_agent_cost_1hz0", sql_dwd_transaction_types_agent_cost_1hz0)

    // gp1.5 计算公式表
    val listGp15 = GoogleSheetsUtils.readGoogleSheet("1czyBLJjyaz-s_qyM4ayMrWVtYQDbyyGLHJZl0dGPR8I", "工作表1!A2:C", null, "site_code,type_code,type_name", null);
    JdbcUtils.executeList(dorisConn, mysqlConn, "dwd_transaction_types_gp1_5", listGp15)
    val sql_dwd_transaction_types_gp1_5_1hz0 =
      """
        |insert into dwd_transaction_types_gp1_5
        |SELECT  '1HZ0' site_code,type_code,type_name,updated_at from  dwd_transaction_types_gp1_5 where site_code ='1HZ'
        |""".stripMargin
    JdbcUtils.execute(dorisConn, "sql_dwd_transaction_types_gp1_5_1hz0", sql_dwd_transaction_types_gp1_5_1hz0)

    // gp2 计算公式表
    val listGp2 = GoogleSheetsUtils.readGoogleSheet("13mH8JvhsrPT8hfjFaMHK7MVk2VT9Vp2Q0bUYOlmJQhU", "工作表1!A2:C", null, "site_code,type_code,type_name", null);
    JdbcUtils.executeList(dorisConn, mysqlConn, "dwd_transaction_types_gp2", listGp2)
    val sql_dwd_transaction_types_gp2 =
      """
        |insert into dwd_transaction_types_gp2
        |SELECT site_code,type_code,type_name,updated_at from  dwd_transaction_types_gp1_5
        |""".stripMargin
    val sql_dwd_transaction_types_gp2_1hz0 =
      """
        |insert into dwd_transaction_types_gp2
        |SELECT  '1HZ0' site_code,type_code,type_name,updated_at from  dwd_transaction_types_gp2 where site_code ='1HZ'
        |""".stripMargin
    JdbcUtils.execute(dorisConn, "sql_dwd_transaction_types_gp2", sql_dwd_transaction_types_gp2)
    JdbcUtils.execute(dorisConn, "sql_dwd_transaction_types_gp2_1hz0", sql_dwd_transaction_types_gp2_1hz0)

  }

  def main(args: Array[String]): Unit = {

    // -- 已替换

    val listGp2 = GoogleSheetsUtils.readGoogleSheet("13mH8JvhsrPT8hfjFaMHK7MVk2VT9Vp2Q0bUYOlmJQhU", "工作表1!A2:C", null, "site_code,type_code,type_name", null);

  }

}
