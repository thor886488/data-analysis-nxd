package com.analysis.nxd.doris.ods

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.utils.VerifyDataUtils

import java.sql.Connection

object OdsSynDataPlgPay {
  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = DateUtils.addSecond(endTimeP, 3600)

    val sql_ods_plg_deposit_slips =
      s"""
         |insert  into ods_plg_deposit_slips
         |select created_at,id,order_no,no,amount,platform,channel,gateway,bank,ip,status,updated_at,fee,extra,return_url,notify_url,actual_amount,user_id,bb_fee,bb_amount,bb_actual_amount,platform_created_at,user_fee,user_fee_rate,real_name,currency_type,notify_num,notify_info,tx_no,service_fee, reward_ratio, rank
         |from syn_plg_deposit_slips
         |where   (updated_at>='$startTime' and  updated_at<='$endTime')
         |""".stripMargin

    val sql_ods_plg_deposit_responses =
      s"""
         |insert  into  ods_plg_deposit_responses
         |select  created_at,id,tx_no,no,platform,channel,status,form,updated_at,actual_amount,bb_fee,bb_amount,bb_actual_amount
         |from  syn_plg_deposit_responses
         |where   (updated_at>='$startTime' and  updated_at<='$endTime')
         |""".stripMargin

    val sql_ods_plg_withdrawal_slips =
      s"""
         |insert  into  ods_plg_withdrawal_slips
         |select  created_at,id,order_no,no,amount,bank,bank_province,bank_city,bank_branch,card_no,card_holder,holder_phone,holder_id,platform,channel,ip,status,updated_at,fee,extra,notify_url,platform_created_at,identity,identity_type,real_name,currency_type,bb_address,bb_type,bb_amount,bb_fee,notify_num,notify_info,tx_no,service_fee,reward_ratio
         |from  syn_plg_withdrawal_slips
         |where   (updated_at>='$startTime' and  updated_at<='$endTime')
         |""".stripMargin

    val sql_ods_plg_withdrawal_responses =
      s"""
         |insert  into  ods_plg_withdrawal_responses
         |select  created_at,id,tx_no,no,platform,channel,status,form,updated_at,bb_amount
         |from  syn_plg_withdrawal_responses
         |where   (updated_at>='$startTime' and  updated_at<='$endTime')
         |""".stripMargin

    val sql_ods_tc_u_user_fillmoney =
      s"""
         |insert into  ods_tc_u_user_fillmoney
         |select Platform as site_code,UserId,OrderId,Id,Platform,FillMoneyChannel,ChannelName,Remarks,RequestMoney,RequestTime,OrderDate,ReturnUrl,RechargeStatus,HandleUserId,ResponseMoney,ResponseMsg,ResponseTime,ResponseCode,ServiceCharge,UserChargeServiceMoney,ExternalOrderId,ProductName,ClientSource,Rduid,Ip,AgentId,ParentPath,UserChargeRedMoney,UserChargeUnAvailableMoney
         |from  syn_mysql_tc_u_user_fillmoney
         |where (RequestTime>='$startTime' and  RequestTime<='$endTime') or (ResponseTime>='$startTime' and  ResponseTime<='$endTime')
         |""".stripMargin

    val sql_ods_tc_u_user_funddetail =
      s"""
         |insert into  ods_tc_u_user_funddetail
         |select CreateTime,Platform as site_code,UserId,OrderId,Id,Platform,KeyLine,PayType,AccountType,FundCategory,Summary,PayMoney,BeforeBalance,AfterBalance,SystemAfterBalance,OperatorUserId,AgentId,ParentPath
         |from  syn_mysql_tc_u_user_funddetail
         |where (CreateTime>='$startTime' and  CreateTime<='$endTime')
         |""".stripMargin
    val sql_ods_tc_u_user_withdrawals =
      s"""
         |insert into  ods_tc_u_user_withdrawals
         |select Platform as  site_code,UserId,OrderId,Platform,WithdrawAgentType,WithdrawCategory,WithdrawStatus,RealName,ProvinceName,CityName,BankCode,BankName,BankSubName,BankCardNumber,RequestMoney,ResponseMoney,RequestTime,OrderDate,ResponseUserId,ResponseMsg,ResponseTime,ServiceChargeMoney,ExamineUserId,ExamineTime,PayInfo,PayWay,IsVerify,AgentId,ParentPath
         |from  syn_mysql_tc_u_user_withdrawals
         |where (RequestTime>='$startTime' and  RequestTime<='$endTime')  or (ResponseTime>='$startTime' and  ResponseTime<='$endTime')
         |""".stripMargin

    //  val sql_ods_hc_c_fillmoney =
    //   s"""
    //     |insert into  ods_hc_c_fillmoney
    //   |select 'HC' site_code,UserId,OrderId,FillMoneyAgent,GoodsName,GoodsType,GoodsDescription,IsNeedDelivery,DeliveryAddress,RequestBy,RequestExtensionInfo,RequestMoney,PayMoney,RequestTime,ReturnUrl,NotifyUrl,ShowUrl,Status,ResponseBy,ResponseCode,ResponseMessage,ResponseMoney,ResponseTime,OuterFlowId,SchemeSource,AgentId,rduid,ip
    //   |from  syn_sql_hc_c_fillmoney
    //   |where (RequestTime>='$startTime' and  RequestTime<='$endTime') or (ResponseTime>='$startTime' and  ResponseTime<='$endTime')
    //   |""".stripMargin

    // val sql_ods_hc_c_fund_detail =
    //    s"""
    //    |insert into  ods_hc_c_fund_detail
    //    |select CreateTime,'HC'  site_code,UserId,OrderId,Id,KeyLine,PayType,AccountType,Category,Summary,PayMoney,BeforeBalance,AfterBalance,OperatorId,AgentId
    //    |from  syn_sql_hc_c_fund_detail
    //    |where (CreateTime>='$startTime' and  CreateTime<='$endTime')
    //    |""".stripMargin

    // val sql_ods_hc_c_withdraw =
    // s"""
    //   |insert into  ods_hc_c_withdraw
    //   |select 'HC' site_code,UserId,OrderId,WithdrawAgent,WithdrawCategory,ProvinceName,CityName,BankCode,BankName,BankSubName,BankCardNumber,RequestMoney,RequestTime,Status,ResponseUserId,ResponseMoney,ResponseMessage,ResponseTime,AgentId,RequestRealName,AuditEndTime,AuditResult,WithdrawalsChannel,AuditStartTime,batchNo,batchDate,RiskAuditStatus,rduid,ip,AuditorEndTime,AuditorID
    //    |from  syn_sql_hc_c_withdraw
    //   |where (RequestTime>='$startTime' and  RequestTime<='$endTime') or (ResponseTime>='$startTime' and  ResponseTime<='$endTime')
    //   |""".stripMargin

    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    JdbcUtils.execute(conn, "sql_ods_plg_deposit_slips", sql_ods_plg_deposit_slips)
    JdbcUtils.execute(conn, "sql_ods_plg_deposit_responses", sql_ods_plg_deposit_responses)
    JdbcUtils.execute(conn, "sql_ods_plg_withdrawal_slips", sql_ods_plg_withdrawal_slips)
    JdbcUtils.execute(conn, "sql_ods_plg_withdrawal_responses", sql_ods_plg_withdrawal_responses)

    //一些小的站点和 plg  相关的
    JdbcUtils.execute(conn, "sql_ods_tc_u_user_fillmoney", sql_ods_tc_u_user_fillmoney)
    JdbcUtils.execute(conn, "sql_ods_tc_u_user_funddetail", sql_ods_tc_u_user_funddetail)
    JdbcUtils.execute(conn, "sql_ods_tc_u_user_withdrawals", sql_ods_tc_u_user_withdrawals)
    //   JdbcUtils.execute(conn, "sql_ods_hc_c_fillmoney", sql_ods_hc_c_fillmoney)
    //  JdbcUtils.execute(conn, "sql_ods_hc_c_fund_detail", sql_ods_hc_c_fund_detail)
    //  JdbcUtils.execute(conn, "sql_ods_hc_c_withdraw", sql_ods_hc_c_withdraw)
  }

  /**
   * 数据校验
   *
   * @param startTimeP
   * @param endTimeP
   * @param isDeleteData
   * @param conn
   */
  def verifyData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")

    val sql_syn_plg_deposit_slips_count = s"select   count(1) countData  from syn_plg_deposit_slips where   created_at>='$startTime' and created_at<='$endTime'"
    val sql_ods_plg_deposit_slips_count = s"select   count(1) countData  from ods_plg_deposit_slips where   created_at>='$startTime' and created_at<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_plg_deposit_slips_count", sql_syn_plg_deposit_slips_count, sql_ods_plg_deposit_slips_count, conn)

    val sql_syn_plg_deposit_responses_count = s"select   count(1) countData  from syn_plg_deposit_responses where   created_at>='$startTime' and created_at<='$endTime'"
    val sql_ods_plg_deposit_responses_count = s"select   count(1) countData  from ods_plg_deposit_responses where   created_at>='$startTime' and created_at<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_plg_deposit_responses_count", sql_syn_plg_deposit_responses_count, sql_ods_plg_deposit_responses_count, conn)

    val sql_syn_plg_withdrawal_slips_count = s"select   count(1) countData  from syn_plg_withdrawal_slips where   created_at>='$startTime' and created_at<='$endTime'"
    val sql_ods_plg_withdrawal_slips_count = s"select   count(1) countData  from ods_plg_withdrawal_slips where   created_at>='$startTime' and created_at<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_plg_withdrawal_slips_count", sql_syn_plg_withdrawal_slips_count, sql_ods_plg_withdrawal_slips_count, conn)

    val sql_syn_plg_withdrawal_responses_count = s"select   count(1) countData  from syn_plg_withdrawal_responses where   created_at>='$startTime' and created_at<='$endTime'"
    val sql_ods_plg_withdrawal_responses_count = s"select   count(1) countData  from ods_plg_withdrawal_responses where   created_at>='$startTime' and created_at<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_plg_withdrawal_responses_count", sql_syn_plg_withdrawal_responses_count, sql_ods_plg_withdrawal_responses_count, conn)

    val sql_syn_mysql_tc_u_user_fillmoney_count = s"select   count(1) countData  from syn_mysql_tc_u_user_fillmoney where   RequestTime>='$startTime' and RequestTime<='$endTime'"
    val sql_ods_tc_u_user_fillmoney_count = s"select   count(1) countData  from ods_tc_u_user_fillmoney where   RequestTime>='$startTime' and RequestTime<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_tc_u_user_fillmoney_count", sql_syn_mysql_tc_u_user_fillmoney_count, sql_ods_tc_u_user_fillmoney_count, conn)

    val sql_syn_mysql_tc_u_user_funddetail_count = s"select   count(1) countData  from syn_mysql_tc_u_user_funddetail where   CreateTime>='$startTime' and CreateTime<='$endTime'"
    val sql_ods_tc_u_user_funddetail_count = s"select   count(1) countData  from ods_tc_u_user_funddetail where   CreateTime>='$startTime' and CreateTime<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_tc_u_user_funddetail_count", sql_syn_mysql_tc_u_user_funddetail_count, sql_ods_tc_u_user_funddetail_count, conn)

    val sql_syn_mysql_tc_u_user_withdrawals_count = s"select   count(1) countData  from syn_mysql_tc_u_user_withdrawals where   RequestTime>='$startTime' and RequestTime<='$endTime'"
    val sql_ods_tc_u_user_withdrawals_count = s"select   count(1) countData  from ods_tc_u_user_withdrawals where   RequestTime>='$startTime' and RequestTime<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_tc_u_user_withdrawals_count", sql_syn_mysql_tc_u_user_withdrawals_count, sql_ods_tc_u_user_withdrawals_count, conn)

//    val sql_syn_sql_hc_c_fillmoney_count = s"select   count(1) countData  from syn_sql_hc_c_fillmoney where   RequestTime>='$startTime' and RequestTime<='$endTime'"
//    val sql_ods_hc_c_fillmoney_count = s"select   count(1) countData  from ods_hc_c_fillmoney where   RequestTime>='$startTime' and RequestTime<='$endTime'"
//    VerifyDataUtils.verifyData("sql_ods_hc_c_fillmoney_count", sql_syn_sql_hc_c_fillmoney_count, sql_ods_hc_c_fillmoney_count, conn)

 //   val sql_syn_sql_hc_c_fund_detail_count = s"select   count(1) countData  from syn_sql_hc_c_fund_detail where   CreateTime>='$startTime' and CreateTime<='$endTime'"
 //   val sql_ods_hc_c_fund_detail_count = s"select   count(1) countData  from ods_hc_c_fund_detail where   CreateTime>='$startTime' and CreateTime<='$endTime'"
 //   VerifyDataUtils.verifyData("sql_ods_hc_c_fund_detail_count", sql_syn_sql_hc_c_fund_detail_count, sql_ods_hc_c_fund_detail_count, conn)

//    val sql_syn_sql_hc_c_withdraw_count = s"select   count(1) countData  from syn_sql_hc_c_withdraw where   RequestTime>='$startTime' and RequestTime<='$endTime'"
//    val sql_ods_hc_c_withdraw_count = s"select   count(1) countData  from ods_hc_c_withdraw where   RequestTime>='$startTime' and RequestTime<='$endTime'"
//    VerifyDataUtils.verifyData("sql_ods_hc_c_withdraw_count", sql_syn_sql_hc_c_withdraw_count, sql_ods_hc_c_withdraw_count, conn)

  }

}
