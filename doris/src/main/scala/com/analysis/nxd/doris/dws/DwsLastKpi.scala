package com.analysis.nxd.doris.dws

import com.analysis.nxd.common.utils.JdbcUtils
import org.apache.commons.codec.binary.StringUtils

import java.sql.Connection

object DwsLastKpi {


  def runData(siteCode: String, startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime: String = startTimeP
    val endTime = endTimeP

    val sql_dws_last_logins =
      s"""
         |insert into  dws_last_logins
         |SELECT t.site_code,t.user_id,  t_u.username ,t.id  as id,t.login_date login_time
         |,concat(cast(abs(login_ip & -16777216 )/16777216 as  int ) ,'.',cast((login_ip & 16711680)/65536 as  int ),'.',cast((login_ip & 65280)/256 as  int ),'.',  cast((login_ip & 255)  as  int ) ) as  login_ip
         |,CASE t.channel_id
         |WHEN 100 THEN  'PC_WEB'
         |WHEN 201 THEN  'IOS_WAP'
         |WHEN 202 THEN  'IOS_APPNATIVE'
         |WHEN 203 THEN  'IOS_APPWAP'
         |WHEN 301 THEN  'IPAD_WAP'
         |WHEN 302 THEN  'IPAD_APPNATIVE'
         |WHEN 303 THEN  'IPAD_APPWAP'
         |WHEN 401 THEN  'ANDROID_WAP'
         |WHEN 402 THEN  'ANDROID_APPNATIVE'
         |WHEN 403 THEN  '403'
         |ELSE concat( t.channel_id,'_unKnow') END as  client_platfom
         |,login_date as updated_at
         |from
         |(
         |select * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  login_date desc) rank_time  from  ods_fh4_user_login_log where (login_date>='$startTime' and  login_date<='$endTime')
         |) t
         |join  (select *  from dwd_users) t_u  on  t.user_id=t_u.id  and   t.site_code=t_u.site_code
         |where t.rank_time=1
         |""".stripMargin

    val sql_dws_last_logins_yft =
      s"""
         |insert into  dws_last_logins
         |SELECT t.site_code,t.uid user_id,  t_u.username ,t.uuid  as id,t.create_date login_time
         |,t.ip login_ip
         |,t.client_type client_platfom
         |,t.create_date as updated_at
         |from
         |(
         |select * ,ROW_NUMBER() OVER(PARTITION BY site_code,uid ORDER BY  create_date desc) rank_time  from  ods_yft_login_history  where (create_date>='$startTime' and  create_date<='$endTime')
         |) t
         |join  (select *  from dwd_users) t_u  on  t.uid=t_u.id  and   t.site_code=t_u.site_code
         |where t.rank_time=1
         |""".stripMargin

    val sql_dws_last_deposit =
      s"""
         |insert  into  dws_last_deposit
         |select
         |t.site_code
         |,t.user_id
         |,t.username
         |,t.id
         |,t.sn
         |,t.apply_time
         |,t.apply_amount
         |,t.deposit_time
         |,t.deposit_ip
         |,t.deposit_platfom
         |,t.deposit_channel
         |,t.deposit_mode
         |,t.deposit_amount
         |,t.deposit_used_time
         |,t.is_bind_card
         |,t.updated_at
         |from
         |(
         |select * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  deposit_time desc) rank_time  from  dwd_deposit
         | where    apply_time>='$startTime' and  apply_time<='$endTime'
         |and  deposit_amount>0
         |and  deposit_amount is  not null
         |) t
         |where t.rank_time=1
         |""".stripMargin

    val sql_dws_last_withdraw =
      s"""
         |insert  into  dws_last_withdraw
         |select
         |t.site_code
         |,t.user_id
         |,t.username
         |,t.id
         |,t.sn
         |,t.apply_time
         |,t.apply_amount
         |,t.withdraw_time
         |,t.withdraw_ip
         |,t.withdraw_platfom
         |,t.withdraw_channel
         |,t.withdraw_mode
         |,t.withdraw_amount
         |,t.auditor_id
         |,t.auditor_name
         |,t.withdraw_fee_amount
         |,t.withdraw_used_time
         |,t.appr_time
         |,t.appr2_time
         |,t.appr_used_time
         |,t.updated_at
         |from
         |(
         |select * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  withdraw_time desc) rank_time  from  dwd_withdraw
         | where    apply_time>='$startTime'  and  apply_time<='$endTime'
         |and  withdraw_amount>0
         |and  withdraw_amount is  not null
         |) t
         |where t.rank_time=1
         |""".stripMargin

    val sql_dws_last_user_bank_locked =
      s"""
         |insert  into dws_last_user_bank_locked
         |select  site_code,user_id,'' username,id uuid, over_time,operator,bindcard_type from
         |(
         |SELECT  * ,ROW_NUMBER() OVER(PARTITION BY site_code,user_id ORDER BY  over_time desc) rank_time  from ods_fh4_user_bank_locked
         | where    over_time>='$startTime'  and  over_time<='$endTime'
         |) t  where rank_time=1
         |""".stripMargin


    JdbcUtils.executeSite(siteCode, conn, "sql_dws_last_logins", sql_dws_last_logins)
    JdbcUtils.executeSite(siteCode, conn, "sql_dws_last_deposit", sql_dws_last_deposit)
    JdbcUtils.executeSite(siteCode, conn, "sql_dws_last_withdraw", sql_dws_last_withdraw)
    if ("FH4".equals(siteCode) || siteCode == null) {
      JdbcUtils.executeSite(siteCode, conn, "sql_dws_last_user_bank_locked", sql_dws_last_user_bank_locked)
    }
    JdbcUtils.executeSite(siteCode, conn, "sql_dws_last_logins_yft", sql_dws_last_logins_yft)
  }
}