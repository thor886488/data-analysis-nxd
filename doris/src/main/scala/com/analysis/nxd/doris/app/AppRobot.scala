package com.analysis.nxd.doris.app

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils, SlackBotUtils}

import java.sql.Connection

object AppRobot {
  def runData(siteCode: String, startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val dataHour = endTimeP.substring(0, 13) + ":00:00";
    var dataHourP = dataHour
    if (siteCode.equals("YFT")) {
      dataHourP = DateUtils.addSecond(dataHour, -3600 * 3)
    }
    val sql_app_hour_user_kpi =
      s"""
         |select
         |site_code  `站点`
         |,sum(register_user_count)  `注册人数`
         |,sum(deposit_amount)  `充值金额`
         |,sum(withdraw_amount)  `提现金额`
         |,sum((deposit_amount-withdraw_amount))  `充提差`
         |,sum(turnover_amount)  `投注金额`
         |,count(distinct if(turnover_amount>0,user_id,null)) `投注人数`
         |,sum(prize_amount)  `奖金金额`
         |,sum(lottery_rebates_amount)  `返点金额`
         |,sum(gp1) gp1
         |,concat(ROUND(if(sum(turnover_amount)=0,0,sum(gp1)/sum(turnover_amount ))*100,2),'%')   `gp1%`
         |,sum(activity_amount)  `活动金额`
         |,sum(agent_cost)  `代理总佣金`
         |,sum(total_agent_share_amount) `代理总佣金_分红`
         |,sum(agent_daily_wage_amount)  `代理总佣金_日工资`
         |,sum(agent_rebates_amount)  `代理总佣金_返水`
         |,sum(agent_other_amount)  `代理总佣金_其他费用`
         |,sum(gp1_5)  `gp1.5`
         |,conCAT(ROUND(if(sum(turnover_amount)=0,0,sum(gp1_5)/sum(turnover_amount ))*100,2) ,'%')   `gp1.5%`
         |,sum(gp2)  `gp2`
         |,conCAT(ROUND(if(sum(turnover_amount)=0,0,sum(gp2)/sum(turnover_amount) )*100,2) ,'%')  `gp2%`
         |,sum(third_turnover_amount)  `三方投注金额`
         |,sum(third_profit_amount)  `三方用户盈亏`
         |,sum(third_agent_rebates_amount)  `三方返水`
         |,sum(third_gp1)  `三方gp1`
         |,conCAT(ROUND(if(sum(third_turnover_amount)=0,0,sum(third_gp1)/sum(third_turnover_amount ))*100,2) ,'%') `三方gp1%`
         |from  app_hour_user_kpi
         |where     data_date='$dataHourP' and  is_tester=0
         |GROUP BY site_code
         |order  by  site_code
         |""".stripMargin
    val sql_app_hour_user_yft_kpi =
      s"""
         |select
         |'YFT'  `站点`
         |,sum(register_user_count)  `注册人数`
         |,sum(deposit_amount)  `充值金额`
         |,sum(withdraw_amount)  `提现金额`
         |,sum((deposit_amount-withdraw_amount))  `充提差`
         |,sum(turnover_amount)  `投注金额`
         |,count(distinct if(turnover_amount>0,user_id,null)) `投注人数`
         |,sum(prize_amount)  `奖金金额`
         |,sum(lottery_rebates_amount)  `返点金额`
         |,sum(gp1) gp1
         |,concat(ROUND(if(sum(turnover_amount)=0,0,sum(gp1)/sum(turnover_amount ))*100,2),'%')   `gp1%`
         |,sum(activity_amount)  `活动金额`
         |,sum(agent_cost)  `代理总佣金`
         |,sum(total_agent_share_amount) `代理总佣金_分红`
         |,sum(agent_daily_wage_amount)  `代理总佣金_日工资`
         |,sum(agent_rebates_amount)  `代理总佣金_返水`
         |,sum(agent_other_amount)  `代理总佣金_其他费用`
         |,sum(gp1_5)  `gp1.5`
         |,conCAT(ROUND(if(sum(turnover_amount)=0,0,sum(gp1_5)/sum(turnover_amount ))*100,2) ,'%')   `gp1.5%`
         |,sum(gp2)  `gp2`
         |,conCAT(ROUND(if(sum(turnover_amount)=0,0,sum(gp2)/sum(turnover_amount) )*100,2) ,'%')  `gp2%`
         |,sum(third_turnover_amount)  `三方投注金额`
         |,sum(third_profit_amount)  `三方用户盈亏`
         |,sum(third_agent_rebates_amount)  `三方返水`
         |,sum(third_gp1)  `三方gp1`
         |,conCAT(ROUND(if(sum(third_turnover_amount)=0,0,sum(third_gp1)/sum(third_turnover_amount ))*100,2) ,'%') `三方gp1%`
         |from  app_hour_user_kpi
         |where     data_date='$dataHourP' and  is_tester=0
         |""".stripMargin
    val hour = Integer.valueOf(dataHour.substring(11, 13));
    val hourLater = hour + 1;
    val title = "数据总览|" + dataHour.substring(0, 10).replace("-", "") + "|" + hour + "_" + hourLater + " 运营情况如下 : "

    var str = "";
    if (siteCode.equals("YFT")) {
      val strY = JdbcUtils.queryStr("Y", conn, sql_app_hour_user_kpi, title);
      val strT = JdbcUtils.queryStr("T", conn, sql_app_hour_user_kpi, title);
      val strTotal = JdbcUtils.queryStr(siteCode, conn, sql_app_hour_user_yft_kpi, title);
      str = strY + "---------------------------------------------" + "\r\n" + strT + "---------------------------------------------" + "\r\n" + strTotal;
    } else {
      str = JdbcUtils.queryStr(siteCode, conn, sql_app_hour_user_kpi, title);
    }
    SlackBotUtils.publishSiteMessage(siteCode, str)
  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.executeSite("BM", conn, "use doris_dt", "use doris_dt")
    runData("BM", "2022-03-01 00:00:00", "2022-04-11 10:59:59", false, conn)
    JdbcUtils.close(conn)
  }

}
