package com.analysis.nxd.doris.ods

import java.sql.Connection
import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.ods.OdsSynData1HZ.logger
import com.analysis.nxd.doris.utils.{ThreadPoolUtils, VerifyDataUtils}

object OdsSynDataFH3 {
  def runUserData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = DateUtils.addSecond(startTimeP, -3600 * 24 * 5)
    val endTime = endTimeP
    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")
    val sql_ods_fh3_users =
      s"""
         |insert into   ods_fh3_passport_users
         |select  'FH3' site_code,userid,username,loginpwd,securitypwd,usertype,nickname,language,skin,email,authtoparent,addcount,authadd,lastip,lasttime,registerip,registertime,userrank,rankcreatetime,rankupdate,lgnpwdupdatetime,secpwdupdatetime
         |from  syn_mysql_fh3_passport_users
         |where  (registertime>='$startTime' and  registertime<=date_add('$endTime',1))  or  (lasttime>='$startTime' and  lasttime<=date_add('$endTime',1))
         |""".stripMargin
    val sql_ods_fh3_usertree =
      s"""
         |insert into   ods_fh3_passport_usertree
         |select  'FH3' site_code,userid,username,nickname,usertype,parentid,lvtopid,lvproxyid,parenttree,userrank,isdeleted,isfrozen,frozentype,istester,ocs_status
         |from  syn_mysql_fh3_passport_usertree
         |""".stripMargin
    val sql_ods_fh3_user_viplevel =
      s"""
         |insert into   ods_fh3_user_viplevel
         |select 'FH3' site_code,userid,id,username,level,created
         |from  syn_mysql_fh3_user_viplevel
         |""".stripMargin
    val sql_ods_fh3_userfund =
      s"""
         |insert into   ods_fh3_userfund
         |select 'FH3' site_code,userid,entry,channelid,cashbalance,channelbalance,availablebalance,holdbalance,maxwithdrawal,islocked,locker,lock_reason,lastupdatetime,lastactivetime
         |from  syn_mysql_fh3_userfund
         |where (lastupdatetime>='$startTime' and  lastupdatetime<='$endTime')
         |""".stripMargin

    JdbcUtils.execute(conn, "sql_ods_fh3_users", sql_ods_fh3_users)
    JdbcUtils.execute(conn, "sql_ods_fh3_usertree", sql_ods_fh3_usertree)
    JdbcUtils.execute(conn, "sql_ods_fh3_user_viplevel", sql_ods_fh3_user_viplevel)
    JdbcUtils.execute(conn, "sql_ods_fh3_userfund", sql_ods_fh3_userfund)
  }

  /**
   * @param startTimeP
   * @param endTimeP
   * @param isDeleteData
   * @param conn
   */
  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP
    val startUpdateTimeH = DateUtils.addSecond(startTime, -2 * 24 * 3600)
    val startUpdateTime = DateUtils.addSecond(startTime, -10 * 24 * 3600)
    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")

    val sql_ods_fh3_passport_login_users =
      s"""
         |insert into   ods_fh3_passport_login_users
         |select   login_time,'FH3' site_code,userid,id,come_from,user_name
         |from  syn_mysql_fh3_passport_login_users
         |where (login_time>='$startTime' and  login_time<='$endTime')
         |""".stripMargin

    val sql_ods_fh3_online_load =
      s"""
         |insert into   ods_fh3_online_load
         |select  trans_time,'FH3' site_code,user_id,id,spec_name,user_name,load_type,load_amount,load_fee,fee_type,load_currency,trans_key,reback_time,reback_note,load_status,lost_todo,lost_todo_user,lost_todo_time,lost_status,trans_id,validation_key,islock,pay_name,pay_attr,acc_aid,acc_name,md5code,save_siteid,utime
         |from  syn_mysql_fh3_online_load
         |where (trans_time>='$startUpdateTime' and  trans_time<='$endTime')
         |""".stripMargin

    val sql_ods_fh3_withdrawel =
      s"""
         |insert into   ods_fh3_withdrawel
         |select  accepttime,'FH3' site_code,userid,entry,topproxyid,adminid,cashier_id,cashier,bank_id,bankname,bankcard,paycard_id,province,city,realname,amount,original_amount,is_split,inone_string,fee,status,finishtime,bankcode,clientip,proxyip,description,isdel,isforcompany,errno,notes,identity,dealing_user_id,dealing_user_name,manual_start_time,manual_end_time,pay_time,verify_name,verify_time,auto_start_time,auto_end_time,risk,customer_admin_id,customer_verify_time
         |from  syn_mysql_fh3_withdrawel
         |where (accepttime>='$startUpdateTime' and  accepttime<='$endTime')
         |""".stripMargin

    val sql_ods_fh3_user_bank_info =
      s"""
         |insert into   ods_fh3_user_bank_info
         |select atime,'FH3' site_code,user_id,id,nickname,user_name,email,bank_id,bank_name,province_id,province,city_id,city,branch,account_name,account,status,utime,islock,locker,locktime,unlocker,unlocktime
         |from  syn_mysql_fh3_user_bank_info
         |where (atime>='$startUpdateTime' and  atime<='$endTime')
         |""".stripMargin


    val sql_ods_fh3_passport_orders =
      s"""
         |insert  into  ods_fh3_passport_orders
         |select actiontime,'FH3' site_code,fromuserid,concat('passport_',entry) entry,touserid,agentid,adminid,adminname,concat('passport_',ordertypeid) ordertypeid,title,amount,description,precash,prebalance,prehold,preavailable,cashbalance,channelbalance,holdbalance,availablebalance,clientip,proxyip,times,channelid,transferuserid,transferchannelid,transferorderid,transferstatus,uniquekey
         |from  syn_mysql_fh3_passport_orders
         |where (actiontime>='$startUpdateTimeH' and  actiontime<='$endTime')
         |""".stripMargin
    val sql_ods_fh3_hgame_orders =
      s"""
         |insert into   ods_fh3_hgame_orders
         |select  actiontime,'FH3'  site_code,fromuserid,concat('hagme_',entry) entry,lotteryid,methodid,packageid,taskid,projectid,touserid,agentid,adminid,adminname,concat('hagme_',ordertypeid) ordertypeid,title,amount,description,prebalance,prehold,preavailable,channelbalance,holdbalance,availablebalance,clientip,proxyip,times,channelid,transferuserid,transferchannelid,transferorderid,transferstatus,uniquekey,modes
         |from  syn_mysql_fh3_hgame_orders
         |where (actiontime>='$startUpdateTimeH' and  actiontime<='$endTime')
         |""".stripMargin
    val sql_ods_fh3_hgame_lottery =
      s"""
         |insert into   ods_fh3_hgame_lottery
         |select  'FH3'  site_code,lotteryid,cnname,enname,sorts,lotterytype,issueset,weekcycle,yearlybreakstart,yearlybreakend,mincommissiongap,minprofit,issuerule,description,numberrule,retry,delay,forwardtime,pushtime
         |from  syn_mysql_fh3_hgame_lottery
         |""".stripMargin
    val sql_ods_fh3_hgame_method =
      s"""
         |insert into   ods_fh3_hgame_method
         |select  'FH3'  site_code,methodid,pid,lotteryid,crowdid,methodname,code,jscode,is_special,addslastype,functionname,functionrule,initlockfunc,areatype,maxcodecount,level,nocount,description,isclose,islock,lockname,maxlost,totalmoney,modes,need_expand,source_id,types
         |from  syn_mysql_fh3_hgame_method
         |""".stripMargin
    val sql_ods_fh3_hgame_issueinfo =
      s"""
         |insert into   ods_fh3_hgame_issueinfo
         |select salestart,'FH3'   site_code,issueid,lotteryid,code,issue,belongdate,saleend,canneldeadline,earliestwritetime,writetime,writeid,verifytime,verifyid,rank,statusfetch,statuscode,statusdeduct,statususerpoint,statuscheckbonus,statusbonus,statustasktoproject,statussynced,statuslocks
         |from  syn_mysql_fh3_hgame_issueinfo
         |where (salestart>='$startUpdateTime' and  salestart<='$endTime') or    (belongdate>='$startTime' and  belongdate<='$endTime')
         |""".stripMargin
    val sql_ods_fh3_hgame_projects =
      s"""
         |insert into   ods_fh3_hgame_projects
         |select  writetime,'FH3' site_code,userid,projectid,packageid,taskid,lotteryid,methodid,issue,bonus,code,expandcode,codetype,singleprice,multiple,totalprice,lvtopid,lvtoppoint,lvproxyid,updatetime,deducttime,bonustime,canceltime,isdeduct,iscancel,isgetprize,prizestatus,userip,cdnip,modes,sqlnum,come_from,hashvar
         |from  syn_mysql_fh3_hgame_projects
         |where (writetime>='$startTime' and  writetime<='$endTime')
         |""".stripMargin
    val sql_ods_fh3_hgame_tasks =
      s"""
         |insert into   ods_fh3_hgame_tasks
         |select begintime,'FH3'  site_code,userid,taskid,lotteryid,methodid,packageid,title,codes,codetype,issuecount,finishedcount,cancelcount,singleprice,taskprice,finishprice,cancelprice,beginissue,wincount,updatetime,prize,userdiffpoints,lvtopid,lvtoppoint,lvproxyid,status,stoponwin,userip,cdnip,modes
         |from  syn_mysql_fh3_hgame_tasks
         |where  (begintime>='$startTime' and  begintime<='$endTime')
         |""".stripMargin
    val sql_ods_fh3_lowgame_orders =
      s"""
         |insert into   ods_fh3_lowgame_orders
         |select actiontime,'FH3'  site_code,fromuserid,concat('lowgame_',entry) entry,lotteryid,methodid,taskid,projectid,touserid,agentid,adminid,adminname,concat('lowgame_',ordertypeid) ordertypeid,title,amount,description,prebalance,prehold,preavailable,channelbalance,holdbalance,availablebalance,clientip,proxyip,times,channelid,transferuserid,transferchannelid,transferorderid,transferstatus,uniquekey
         |from  syn_mysql_fh3_lowgame_orders
         |where (actiontime>='$startUpdateTime' and  actiontime<='$endTime')
         |""".stripMargin
    val sql_ods_fh3_lowgame_lottery =
      s"""
         |insert into   ods_fh3_lowgame_lottery
         |select 'FH3' site_code,lotteryid,cnname,enname,lotterytype,dailystart,dailyend,edittime,canceldeadline,dynamicprizestart,dynamicprizeend,weekcycle,yearlybreakstart,yearlybreakend,mincommissiongap,minprofit,issuerule,description,numberrule,adjustminprofit,adjustmaxpercent,issetpool,isprizedynamic
         |from  syn_mysql_fh3_lowgame_lottery
         |""".stripMargin

    val sql_ods_fh3_lowgame_method =
      s"""
         |insert into   ods_fh3_lowgame_method
         |select 'FH3' site_code,methodid,pid,lotteryid,methodname,functionname,isprizedynamic,locksid,areatype,level,nocount,description,isclose,totalmoney,issetpool
         |from  syn_mysql_fh3_lowgame_method
         |""".stripMargin

    val sql_ods_fh3_lowgame_issueinfo =
      s"""
         |insert into   ods_fh3_lowgame_issueinfo
         |select salestart,'FH3'  site_code,issueid,lotteryid,code,issue,saleend,dynamicprizestart,dynamicprizeend,canneldeadline,officialtime,writetime,writeid,verifytime,verifyid,statuscode,statusdeduct,statususerpoint,statusdoublecheck,statuscheckbonus,statusbonus,statustasktoproject,statuslocks,bonus_audit_status,bonus_auditor,bonus_audit_date
         |from  syn_mysql_fh3_lowgame_issueinfo
         |where (salestart>='$startUpdateTime' and  salestart<='$endTime')
         |""".stripMargin

    val sql_ods_fh3_lowgame_projects =
      s"""
         |insert into   ods_fh3_lowgame_projects
         |select writetime,'FH3'  site_code,userid,projectid,taskid,lotteryid,methodid,issue,isdynamicprize,bonus,code,singleprice,multiple,totalprice,lvtopid,lvtoppoint,lvproxyid,updatetime,isdeduct,iscancel,isgetprize,prizestatus,userip,cdnip,hashvar
         |from  syn_mysql_fh3_lowgame_projects
         |where (writetime>='$startTime' and  writetime<='$endTime')
         |""".stripMargin

    val sql_ods_fh3_lowgame_tasks =
      s"""
         |insert into   ods_fh3_lowgame_tasks
         |select begintime,'FH3' site_code,userid,taskid,lotteryid,methodid,title,codes,issuecount,finishedcount,cancelcount,singleprice,taskprice,finishprice,cancelprice,beginissue,wincount,updatetime,prize,userdiffpoints,lvtopid,lvtoppoint,lvproxyid,status,stoponwin,userip,cdnip
         |from  syn_mysql_fh3_lowgame_tasks
         |where  (begintime>='$startTime' and  begintime<='$endTime')
         |""".stripMargin

    val sql_ods_fh3_ccb_deposit_record =
      s"""
         |insert into ods_fh3_ccb_deposit_record
         |select  created,'FH3' site_code,user_id,id,user_name,topproxy_name,money,account_id,account,account_name,payacc_id,accept_name,accept_card,transfer_id,status,admin_id,admin_name,error_type,remark,deal_time,modified,add_money_time
         |from  syn_mysql_fh3_ccb_deposit_record
         |where  (created>='$startUpdateTime' and  created<='$endTime')
         |""".stripMargin
    val sql_ods_fh3_ccb2cmb_deposit_record =
      s"""
         |insert into ods_fh3_ccb2cmb_deposit_record
         |select  created,'FH3' site_code,user_id,id,user_name,topproxy_name,bank_name,money,account_id,account,account_name,payacc_id,accept_name,accept_card,accept_area,`KEY`
         |,transfer_id,status,admin_id,admin_name,error_type,remark,deal_time,modified,add_money_time
         |from  syn_mysql_fh3_ccb2cmb_deposit_record
         |where  (created>='$startUpdateTime' and  created<='$endTime')
         |""".stripMargin
    val sql_ods_fh3_cmbkh_deposit_record =
      s"""
         |insert into ods_fh3_cmbkh_deposit_record
         |select created,'FH3'  site_code,user_id,id,user_name,topproxy_name,bank_name,money,account_id,account,account_name,payacc_id,accept_name,accept_card,accept_area,`KEY`,transfer_id,status,admin_id,admin_name,error_type,remark,deal_time,modified,add_money_time
         |from  syn_mysql_fh3_cmbkh_deposit_record
         |where  (created>='$startUpdateTime' and  created<='$endTime')
         |""".stripMargin
    val sql_ods_fh3_cmb_deposit_record =
      s"""
         |insert into ods_fh3_cmb_deposit_record
         |select created,'FH3'  site_code,user_id,id,user_name,topproxy_name,bank_name,money,account_id,account,account_name,payacc_id,accept_name,accept_card,accept_area,transfer_id,status,admin_id,admin_name,error_type,remark,deal_time,modified,add_money_time
         |from  syn_mysql_fh3_cmb_deposit_record
         |where  (created>='$startUpdateTime' and  created<='$endTime')
         |""".stripMargin
    val sql_ods_fh3_all2cmb_deposit_record =
      s"""
         |insert into ods_fh3_all2cmb_deposit_record
         |select  created,'FH3'  site_code,user_id,id,user_name,topproxy_name,bank_name,money,account_id,account,account_name,payacc_id,accept_name,accept_card,accept_area,`KEY`,transfer_id,status,admin_id,admin_name,error_type,remark,deal_time,modified,add_money_time
         |from  syn_mysql_fh3_all2cmb_deposit_record
         |where  (created>='$startUpdateTime' and  created<='$endTime')
         |""".stripMargin
    val sql_ods_fh3_gem_deposit_record =
      s"""
         |insert into ods_fh3_gem_deposit_record
         |select   created,'FH3'  site_code,userid,id,amount,request_date,client_order_id,gem_order_id,status,pay_date,jb_time,user_name,topproxy_name,gem_response_date,apply_bank,gateway
         |from  syn_mysql_fh3_gem_deposit_record
         |where  (created>='$startUpdateTime' and  created<='$endTime')
         |""".stripMargin
    val sql_ods_fh3_fm_deposit_record =
      s"""
         |insert into ods_fh3_fm_deposit_record
         |select  created,'FH3' site_code,userid,id,amount,request_date,client_order_id,fn_order_id,status,pay_date,jb_time,user_name,topproxy_name,fm_response_date,apply_bank,apply_bank_id
         |from  syn_mysql_fh3_fm_deposit_record
         |where  (created>='$startUpdateTime' and  created<='$endTime')
         |""".stripMargin
    val sql_ods_fh3_mc_large_withdrawel =
      s"""
         |insert  into ods_fh3_mc_large_withdrawel
         |select  accepttime,'FH3' site_code,userid,entry,topproxyid,adminid,cashier_id,cashier,bank_id,bankname,bankcard,paycard_id,province,city,branch,realname,amount,original_amount,is_split,inone_string,fee,status,finishtime,bankcode,clientip,proxyip,description,isdel,isforcompany,errno,notes,identity,dealing_user_id,dealing_user_name,manual_start_time,manual_end_time,pay_time,verify_name,verify_time,auto_start_time,auto_end_time,risk,customer_admin_id,customer_verify_time,review_admin_id,review_time,ubankid,pay_type
         |from  syn_mysql_fh3_mc_large_withdrawel
         |where  (accepttime>='$startUpdateTime' and  accepttime<='$endTime')
         |""".stripMargin

    val sql_ods_fh3_jbp_withdraw_record =
      """
        |insert  into ods_fh3_jbp_withdraw_record
        |select  'FH3' site_code,userid,id,request_date,amount,bankcard,company_order_num,mownecum_order_num,error_msg,status,transaction_charge,withdraw_id,created,bankid
        |from  syn_mysql_fh3_jbp_withdraw_record
        |""".stripMargin
    val sql_ods_fh3_gem_withdraw_record =
      """
        |insert  into ods_fh3_gem_withdraw_record
        |select  'FH3' site_code,userid,id,request_date,amount,bankcard,company_order_num,gem_order_num,error_msg,status,transaction_charge,withdraw_id,created,bankid
        |from  syn_mysql_fh3_gem_withdraw_record
        |""".stripMargin
    val sql_ods_fh3_yizf_withdraw_record =
      """
        |insert  into ods_fh3_yizf_withdraw_record
        |select  'FH3' site_code,userid,id,request_date,amount,bankcard,company_order_num,yizf_order_num,error_msg,status,transaction_charge,withdraw_id,created,bankid
        |from  syn_mysql_fh3_yizf_withdraw_record
        |""".stripMargin

    val sql_ods_fh3_lowgame_ordertype =
      s"""
         |insert into ods_fh3_ordertype
         |select  'FH3' site_code,concat('lowgame_',id),cntitle,entitle,operations,parentid,description
         |from  syn_mysql_fh3_lowgame_ordertype
         |""".stripMargin
    val sql_ods_fh3_hagme_ordertype =
      s"""
         |insert into ods_fh3_ordertype
         |select  'FH3' site_code,concat('hagme_',id),cntitle,entitle,operations,parentid,description
         |from  syn_mysql_fh3_hgame_ordertype
         |""".stripMargin
    val sql_ods_fh3_passport_ordertype =
      s"""
         |insert into ods_fh3_ordertype
         |select  'FH3' site_code,concat('passport_',id),cntitle,entitle,operations,parentid,description
         |from  syn_mysql_fh3_passport_ordertype
         |""".stripMargin

    val sql_ods_fh3_passport_adminuser =
      """
        |insert into  ods_fh3_passport_adminuser
        |select 'FH3' site_code,adminid,adminname,adminnick,adminpass,adminlang,groupid,sn_id,menustrs,islocked
        |from  syn_mysql_fh3_passport_adminuser
        |""".stripMargin

    JdbcUtils.execute(conn, "sql_ods_fh3_passport_login_users", sql_ods_fh3_passport_login_users)
    JdbcUtils.execute(conn, "sql_ods_fh3_online_load", sql_ods_fh3_online_load)
    JdbcUtils.execute(conn, "sql_ods_fh3_withdrawel", sql_ods_fh3_withdrawel)
    JdbcUtils.execute(conn, "sql_ods_fh3_user_bank_info", sql_ods_fh3_user_bank_info)
    JdbcUtils.execute(conn, "sql_ods_fh3_passport_orders", sql_ods_fh3_passport_orders)
    JdbcUtils.execute(conn, "sql_ods_fh3_hgame_orders", sql_ods_fh3_hgame_orders)
    JdbcUtils.execute(conn, "sql_ods_fh3_hgame_issueinfo", sql_ods_fh3_hgame_issueinfo)
    JdbcUtils.execute(conn, "sql_ods_fh3_hgame_projects", sql_ods_fh3_hgame_projects)
    JdbcUtils.execute(conn, "sql_ods_fh3_hgame_tasks", sql_ods_fh3_hgame_tasks)
    JdbcUtils.execute(conn, "sql_ods_fh3_hgame_lottery", sql_ods_fh3_hgame_lottery)
    JdbcUtils.execute(conn, "sql_ods_fh3_hgame_method", sql_ods_fh3_hgame_method)
    JdbcUtils.execute(conn, "sql_ods_fh3_lowgame_orders", sql_ods_fh3_lowgame_orders)
    JdbcUtils.execute(conn, "sql_ods_fh3_lowgame_issueinfo", sql_ods_fh3_lowgame_issueinfo)
    JdbcUtils.execute(conn, "sql_ods_fh3_lowgame_projects", sql_ods_fh3_lowgame_projects)
    JdbcUtils.execute(conn, "sql_ods_fh3_lowgame_tasks", sql_ods_fh3_lowgame_tasks)
    JdbcUtils.execute(conn, "sql_ods_fh3_lowgame_lottery", sql_ods_fh3_lowgame_lottery)
    JdbcUtils.execute(conn, "sql_ods_fh3_lowgame_method", sql_ods_fh3_lowgame_method)
    JdbcUtils.execute(conn, "sql_ods_fh3_lowgame_ordertype", sql_ods_fh3_lowgame_ordertype)
    JdbcUtils.execute(conn, "sql_ods_fh3_hagme_ordertype", sql_ods_fh3_hagme_ordertype)
    JdbcUtils.execute(conn, "sql_ods_fh3_passport_ordertype", sql_ods_fh3_passport_ordertype)
    JdbcUtils.execute(conn, "sql_ods_fh3_gem_deposit_record", sql_ods_fh3_gem_deposit_record)
    JdbcUtils.execute(conn, "sql_ods_fh3_mc_large_withdrawel", sql_ods_fh3_mc_large_withdrawel)
    JdbcUtils.execute(conn, "sql_ods_fh3_jbp_withdraw_record", sql_ods_fh3_jbp_withdraw_record)
    JdbcUtils.execute(conn, "sql_ods_fh3_gem_withdraw_record", sql_ods_fh3_gem_withdraw_record)
    JdbcUtils.execute(conn, "sql_ods_fh3_yizf_withdraw_record", sql_ods_fh3_yizf_withdraw_record)
    JdbcUtils.execute(conn, "sql_ods_fh3_passport_adminuser", sql_ods_fh3_passport_adminuser)
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
    val sql_syn_fh3_passport_users_count = s"select   count(1) countData  from syn_mysql_fh3_passport_users where  registertime>='$startTime' and   registertime<='$endTime'"
    val sql_ods_fh3_passport_users_count = s"select   count(1) countData  from ods_fh3_passport_users where   registertime>='$startTime' and  registertime<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_2hzn_users_count", sql_syn_fh3_passport_users_count, sql_ods_fh3_passport_users_count, conn)

    val sql_syn_mysql_fh3_passport_orders_count = s"select   count(1) countData  from syn_mysql_fh3_passport_orders where   actiontime>='$startTime' and actiontime<='$endTime'"
    val sql_ods_fh3_passport_orders_count = s"select   count(1) countData  from ods_fh3_passport_orders where   actiontime>='$startTime' and actiontime<='$endTime'"
    VerifyDataUtils.verifyData("sql_ods_fh3_passport_orders_count", sql_syn_mysql_fh3_passport_orders_count, sql_ods_fh3_passport_orders_count, conn)

  }


}
