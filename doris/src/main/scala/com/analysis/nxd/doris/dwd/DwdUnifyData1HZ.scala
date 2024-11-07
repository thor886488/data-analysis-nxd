package com.analysis.nxd.doris.dwd

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.utils.ThreadPoolUtils
import org.slf4j.LoggerFactory

import java.sql.Connection

object DwdUnifyData1HZ {

  val logger = LoggerFactory.getLogger(DwdUnifyData1HZ.getClass)

  /**
   * 因为用户数据是所有数据的基础，所以用户数据单独抽取出来
   *
   * @param startTimeP
   * @param endTimeP
   * @param isDeleteData
   * @param conn
   */
  def runUserData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = DateUtils.addSecond(startTimeP, -3600 * 24 * 5)
    val endTime = endTimeP

    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")
    val sql_dwd_1hz0_users =
      s"""
         |INSERT INTO dwd_users
         |select
         |'1HZ0' site_code
         |,t_u.userid id
         |,t_u.username
         |,t_u.registertime  created_at
         |,t_t.nickname nick_name
         |,null real_name
         |,t_u.email email
         |,null mobile
         |,null qq
         |,null skype
         |,t_u.registerip ip
         |,if(t_t.usertype>=1,1,0) is_agent
         |,t_t.istester is_tester
         |,t_t.parentid parent_id
         |,t_t.parent_username
         |,t_t.user_chain_names
         |,(find_in_set(t_t.username,regexp_replace(t_t.user_chain_names,'/',',') )-2) as user_level
         |,ifnull(t_v.isused,0) vip_level
         |,ifnull(t_v.isused,0) is_vip
         |,if(t_t.isfrozen>=1,1,0) is_freeze
         |,0 is_joint
         |,lasttime  updated_at
         |from
         |ods_1hz_users t_u
         |join  (
         |  select t_u.*
         |  ,t_p_1.username parent_username
         |  ,CONCAT(
         |  if(null_or_empty(t_p_49.username),'',CONCAT('/',t_p_49.username))
         |  ,if(null_or_empty(t_p_48.username),'',CONCAT('/',t_p_48.username))
         |  ,if(null_or_empty(t_p_47.username),'',CONCAT('/',t_p_47.username))
         |  ,if(null_or_empty(t_p_46.username),'',CONCAT('/',t_p_46.username))
         |  ,if(null_or_empty(t_p_45.username),'',CONCAT('/',t_p_45.username))
         |  ,if(null_or_empty(t_p_44.username),'',CONCAT('/',t_p_44.username))
         |  ,if(null_or_empty(t_p_43.username),'',CONCAT('/',t_p_43.username))
         |  ,if(null_or_empty(t_p_42.username),'',CONCAT('/',t_p_42.username))
         |  ,if(null_or_empty(t_p_41.username),'',CONCAT('/',t_p_41.username))
         |  ,if(null_or_empty(t_p_40.username),'',CONCAT('/',t_p_40.username))
         |  ,if(null_or_empty(t_p_39.username),'',CONCAT('/',t_p_39.username))
         |  ,if(null_or_empty(t_p_38.username),'',CONCAT('/',t_p_38.username))
         |  ,if(null_or_empty(t_p_37.username),'',CONCAT('/',t_p_37.username))
         |  ,if(null_or_empty(t_p_36.username),'',CONCAT('/',t_p_36.username))
         |  ,if(null_or_empty(t_p_35.username),'',CONCAT('/',t_p_35.username))
         |  ,if(null_or_empty(t_p_34.username),'',CONCAT('/',t_p_34.username))
         |  ,if(null_or_empty(t_p_33.username),'',CONCAT('/',t_p_33.username))
         |  ,if(null_or_empty(t_p_32.username),'',CONCAT('/',t_p_32.username))
         |  ,if(null_or_empty(t_p_31.username),'',CONCAT('/',t_p_31.username))
         |  ,if(null_or_empty(t_p_30.username),'',CONCAT('/',t_p_30.username))
         |  ,if(null_or_empty(t_p_29.username),'',CONCAT('/',t_p_29.username))
         |  ,if(null_or_empty(t_p_28.username),'',CONCAT('/',t_p_28.username))
         |  ,if(null_or_empty(t_p_27.username),'',CONCAT('/',t_p_27.username))
         |  ,if(null_or_empty(t_p_26.username),'',CONCAT('/',t_p_26.username))
         |  ,if(null_or_empty(t_p_25.username),'',CONCAT('/',t_p_25.username))
         |  ,if(null_or_empty(t_p_24.username),'',CONCAT('/',t_p_24.username))
         |  ,if(null_or_empty(t_p_23.username),'',CONCAT('/',t_p_23.username))
         |  ,if(null_or_empty(t_p_22.username),'',CONCAT('/',t_p_22.username))
         |  ,if(null_or_empty(t_p_21.username),'',CONCAT('/',t_p_21.username))
         |  ,if(null_or_empty(t_p_20.username),'',CONCAT('/',t_p_20.username))
         |  ,if(null_or_empty(t_p_19.username),'',CONCAT('/',t_p_19.username))
         |  ,if(null_or_empty(t_p_18.username),'',CONCAT('/',t_p_18.username))
         |  ,if(null_or_empty(t_p_17.username),'',CONCAT('/',t_p_17.username))
         |  ,if(null_or_empty(t_p_16.username),'',CONCAT('/',t_p_16.username))
         |  ,if(null_or_empty(t_p_15.username),'',CONCAT('/',t_p_15.username))
         |  ,if(null_or_empty(t_p_14.username),'',CONCAT('/',t_p_14.username))
         |  ,if(null_or_empty(t_p_13.username),'',CONCAT('/',t_p_13.username))
         |  ,if(null_or_empty(t_p_12.username),'',CONCAT('/',t_p_12.username))
         |  ,if(null_or_empty(t_p_11.username),'',CONCAT('/',t_p_11.username))
         |  ,if(null_or_empty(t_p_10.username),'',CONCAT('/',t_p_10.username))
         |  ,if(null_or_empty(t_p_9.username),'',CONCAT('/',t_p_9.username))
         |  ,if(null_or_empty(t_p_8.username),'',CONCAT('/',t_p_8.username))
         |  ,if(null_or_empty(t_p_7.username),'',CONCAT('/',t_p_7.username))
         |  ,if(null_or_empty(t_p_6.username),'',CONCAT('/',t_p_6.username))
         |  ,if(null_or_empty(t_p_5.username),'',CONCAT('/',t_p_5.username))
         |  ,if(null_or_empty(t_p_4.username),'',CONCAT('/',t_p_4.username))
         |  ,if(null_or_empty(t_p_3.username),'',CONCAT('/',t_p_3.username))
         |  ,if(null_or_empty(t_p_2.username),'',CONCAT('/',t_p_2.username))
         |  ,if(null_or_empty(t_p_1.username),'',CONCAT('/',t_p_1.username))
         |  ,CONCAT('/',t_u.username,'/')
         |  ) as user_chain_names
         |  from
         |  (select * from ods_1hz_usertree  where  split_part(parenttree,',',50) is  null  ) t_u
         |  left join ods_1hz_usertree t_p_1 on t_u.parentid=t_p_1.userid  and  t_u.site_code=t_p_1.site_code
         |  left join ods_1hz_usertree t_p_2 on t_p_1.parentid=t_p_2.userid  and  t_p_1.site_code=t_p_2.site_code
         |  left join ods_1hz_usertree t_p_3 on t_p_2.parentid=t_p_3.userid  and  t_p_2.site_code=t_p_3.site_code
         |  left join ods_1hz_usertree t_p_4 on t_p_3.parentid=t_p_4.userid  and  t_p_3.site_code=t_p_4.site_code
         |  left join ods_1hz_usertree t_p_5 on t_p_4.parentid=t_p_5.userid  and  t_p_4.site_code=t_p_5.site_code
         |  left join ods_1hz_usertree t_p_6 on t_p_5.parentid=t_p_6.userid  and  t_p_5.site_code=t_p_6.site_code
         |  left join ods_1hz_usertree t_p_7 on t_p_6.parentid=t_p_7.userid  and  t_p_6.site_code=t_p_7.site_code
         |  left join ods_1hz_usertree t_p_8 on t_p_7.parentid=t_p_8.userid  and  t_p_7.site_code=t_p_8.site_code
         |  left join ods_1hz_usertree t_p_9 on t_p_8.parentid=t_p_9.userid  and  t_p_8.site_code=t_p_9.site_code
         |  left join ods_1hz_usertree t_p_10 on t_p_9.parentid=t_p_10.userid  and  t_p_9.site_code=t_p_10.site_code
         |  left join ods_1hz_usertree t_p_11 on t_p_10.parentid=t_p_11.userid  and  t_p_10.site_code=t_p_11.site_code
         |  left join ods_1hz_usertree t_p_12 on t_p_11.parentid=t_p_12.userid  and  t_p_11.site_code=t_p_12.site_code
         |  left join ods_1hz_usertree t_p_13 on t_p_12.parentid=t_p_13.userid  and  t_p_12.site_code=t_p_13.site_code
         |  left join ods_1hz_usertree t_p_14 on t_p_13.parentid=t_p_14.userid  and  t_p_13.site_code=t_p_14.site_code
         |  left join ods_1hz_usertree t_p_15 on t_p_14.parentid=t_p_15.userid  and  t_p_14.site_code=t_p_15.site_code
         |  left join ods_1hz_usertree t_p_16 on t_p_15.parentid=t_p_16.userid  and  t_p_15.site_code=t_p_16.site_code
         |  left join ods_1hz_usertree t_p_17 on t_p_16.parentid=t_p_17.userid  and  t_p_16.site_code=t_p_17.site_code
         |  left join ods_1hz_usertree t_p_18 on t_p_17.parentid=t_p_18.userid  and  t_p_17.site_code=t_p_18.site_code
         |  left join ods_1hz_usertree t_p_19 on t_p_18.parentid=t_p_19.userid  and  t_p_18.site_code=t_p_19.site_code
         |  left join ods_1hz_usertree t_p_20 on t_p_19.parentid=t_p_20.userid  and  t_p_19.site_code=t_p_20.site_code
         |  left join ods_1hz_usertree t_p_21 on t_p_20.parentid=t_p_21.userid  and  t_p_20.site_code=t_p_21.site_code
         |  left join ods_1hz_usertree t_p_22 on t_p_21.parentid=t_p_22.userid  and  t_p_21.site_code=t_p_22.site_code
         |  left join ods_1hz_usertree t_p_23 on t_p_22.parentid=t_p_23.userid  and  t_p_22.site_code=t_p_23.site_code
         |  left join ods_1hz_usertree t_p_24 on t_p_23.parentid=t_p_24.userid  and  t_p_23.site_code=t_p_24.site_code
         |  left join ods_1hz_usertree t_p_25 on t_p_24.parentid=t_p_25.userid  and  t_p_24.site_code=t_p_25.site_code
         |  left join ods_1hz_usertree t_p_26 on t_p_25.parentid=t_p_26.userid  and  t_p_25.site_code=t_p_26.site_code
         |  left join ods_1hz_usertree t_p_27 on t_p_26.parentid=t_p_27.userid  and  t_p_26.site_code=t_p_27.site_code
         |  left join ods_1hz_usertree t_p_28 on t_p_27.parentid=t_p_28.userid  and  t_p_27.site_code=t_p_28.site_code
         |  left join ods_1hz_usertree t_p_29 on t_p_28.parentid=t_p_29.userid  and  t_p_28.site_code=t_p_29.site_code
         |  left join ods_1hz_usertree t_p_30 on t_p_29.parentid=t_p_30.userid  and  t_p_29.site_code=t_p_30.site_code
         |  left join ods_1hz_usertree t_p_31 on t_p_30.parentid=t_p_31.userid  and  t_p_30.site_code=t_p_31.site_code
         |  left join ods_1hz_usertree t_p_32 on t_p_31.parentid=t_p_32.userid  and  t_p_31.site_code=t_p_32.site_code
         |  left join ods_1hz_usertree t_p_33 on t_p_32.parentid=t_p_33.userid  and  t_p_32.site_code=t_p_33.site_code
         |  left join ods_1hz_usertree t_p_34 on t_p_33.parentid=t_p_34.userid  and  t_p_33.site_code=t_p_34.site_code
         |  left join ods_1hz_usertree t_p_35 on t_p_34.parentid=t_p_35.userid  and  t_p_34.site_code=t_p_35.site_code
         |  left join ods_1hz_usertree t_p_36 on t_p_35.parentid=t_p_36.userid  and  t_p_35.site_code=t_p_36.site_code
         |  left join ods_1hz_usertree t_p_37 on t_p_36.parentid=t_p_37.userid  and  t_p_36.site_code=t_p_37.site_code
         |  left join ods_1hz_usertree t_p_38 on t_p_37.parentid=t_p_38.userid  and  t_p_37.site_code=t_p_38.site_code
         |  left join ods_1hz_usertree t_p_39 on t_p_38.parentid=t_p_39.userid  and  t_p_38.site_code=t_p_39.site_code
         |  left join ods_1hz_usertree t_p_40 on t_p_39.parentid=t_p_40.userid  and  t_p_39.site_code=t_p_40.site_code
         |  left join ods_1hz_usertree t_p_41 on t_p_40.parentid=t_p_41.userid  and  t_p_40.site_code=t_p_41.site_code
         |  left join ods_1hz_usertree t_p_42 on t_p_41.parentid=t_p_42.userid  and  t_p_41.site_code=t_p_42.site_code
         |  left join ods_1hz_usertree t_p_43 on t_p_42.parentid=t_p_43.userid  and  t_p_42.site_code=t_p_43.site_code
         |  left join ods_1hz_usertree t_p_44 on t_p_43.parentid=t_p_44.userid  and  t_p_43.site_code=t_p_44.site_code
         |  left join ods_1hz_usertree t_p_45 on t_p_44.parentid=t_p_45.userid  and  t_p_44.site_code=t_p_45.site_code
         |  left join ods_1hz_usertree t_p_46 on t_p_45.parentid=t_p_46.userid  and  t_p_45.site_code=t_p_46.site_code
         |  left join ods_1hz_usertree t_p_47 on t_p_46.parentid=t_p_47.userid  and  t_p_46.site_code=t_p_47.site_code
         |  left join ods_1hz_usertree t_p_48 on t_p_47.parentid=t_p_48.userid  and  t_p_47.site_code=t_p_48.site_code
         |  left join ods_1hz_usertree t_p_49 on t_p_48.parentid=t_p_49.userid  and  t_p_48.site_code=t_p_49.site_code
         |) t_t  on  t_u.site_code = t_t.site_code and   t_u.userid = t_t.userid
         |left join (select *  from  ods_1hz_user_vip where  isused=1) t_v  on   t_u.site_code = t_v.site_code and   t_u.userid = t_v.userid
         |""".stripMargin

    val sql_dwd_1hz_users =
      s"""
         |insert  into  dwd_users
         |select '1HZ'   site_code,id,username,date_sub(created_at, INTERVAL 6 HOUR),nick_name,real_name,email,mobile,qq,skype,ip,is_agent,is_tester,parent_id,parent_username,user_chain_names,user_level,vip_level,is_vip,is_joint,is_freeze,date_sub(updated_at, INTERVAL 6 HOUR)
         |from dwd_users
         |where  site_code='1HZ0'
         |""".stripMargin

    val start = System.currentTimeMillis()
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    JdbcUtils.execute(conn, "sql_dwd_1hz0_users", sql_dwd_1hz0_users)
    JdbcUtils.execute(conn, "sql_dwd_1hz_users", sql_dwd_1hz_users)

    val end = System.currentTimeMillis()
    logger.info("1HZ0 user 数据归一累计耗时(毫秒):" + (end - start))

  }

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP
    logger.warn(s" startTime : '$startTime'  , endTime '$endTime', isDeleteData '$isDeleteData'")

    val sql_dwd_1hz0_transactions =
      s"""
         |INSERT INTO dwd_transactions
         |select
         |t.times created_at
         |,'1HZ0'  site_code
         |,t.fromuserid user_id
         |,t_u.username
         |,t.entry uuid
         |,t.entry tran_no
         |,t.projectid project_no
         |,t.taskid trace_no
         |,t.ordertypeid type_code
         |,ifnull(t_t.paren_type_code,'')  paren_type_code
         |,ifnull(t_t.paren_type_name,'')  paren_type_name
         |,t.title type_name
         |,t.lotteryid as series_code
         |,t_l.cnname series_name
         |,t.lotteryid as lottery_code
         |,t_l.cnname lottery_name
         |,t.methodid as turnover_code
         |,t_m.methodname as turnover_name
         |,'UNKNOW' as   issue
         |,'UNKNOW' as   issue_web
         |,'UNKNOW' as   issue_date
         |,t.amount
         |,t.channelbalance
         |,0 is_first_deposit
         |,0 is_first_withdraw
         |,0 as is_first_turnover
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t_u.vip_level
         |,t_u.is_vip
         |,t_u.is_joint
         |,t_u.created_at user_created_at
         |,t.times updated_at
         |from
         |( select * from ods_1hz_orders where   (times>='$startTime' and  times<='$endTime')  ) t
         |join  (select *  from doris_dt.dwd_users where  site_code='1HZ')  t_u  on t.site_code=t_u.site_code  and   t.fromuserid=t_u.id 
         |left join  (select * from dwd_transaction_types ) t_t   on  t.ordertypeid=t_t.type_code and   t.site_code=t_t.site_code
         |left join  (select distinct  lotteryid,cnname from ods_1hz_lottery ) t_l  on  t.lotteryid=t_l.lotteryid
         |left join  (select distinct  lotteryid,methodid,methodname from ods_1hz_method ) t_m  on   t.lotteryid=t_m.lotteryid  and t.methodid=t_m.methodid
         |""".stripMargin

    val sql_dwd_1hz0_user_logins =
      s"""
         |INSERT INTO dwd_user_logins
         |select t_l.logindate,'1HZ0'  site_code,t_l.userid user_id,t_u.username ,t_l.id as  uuid,t_l.loginip ip,''  location,null as client_type,0 is_first_login,t_u.is_agent,t_u.is_tester,t_u.parent_id,t_u.parent_username ,t_u.user_chain_names,t_u.user_level
         |,t_u.vip_level
         |,t_u.is_vip
         |,t_u.is_joint
         |,t_u.created_at user_created_at,t_l.logindate
         |from
         |(select *  from  ods_1hz_login_log where logindate>='$startTime' and  logindate<='$endTime' ) t_l
         |join  (select *  from doris_dt.dwd_users where  site_code='1HZ')  t_u  on  t_l.userid=t_u.id and  t_l.site_code=t_u.site_code 
         |""".stripMargin

    val sql_dwd_1hz0_user_bank =
      s"""
         |INSERT INTO dwd_user_bank
         |select '1HZ0' site_code,t.user_id,t_u.username ,t.id  as uuid
         |,t.atime  created_at
         |,'一般绑卡' as  bindcard_type
         |,t.branch branch_name
         |,t_u.is_agent,t_u.is_tester,t_u.parent_id ,t_u.parent_username ,t_u.user_chain_names,t_u.user_level
         |,t_u.vip_level
         |,t_u.is_vip
         |,t_u.is_joint
         |,t_u.created_at user_created_at,t.atime  as update_date
         |from
         |(select *  from  ods_1hz_user_bank_info where atime>='$startTime' and  atime<='$endTime'  ) t
         |join  (select *  from doris_dt.dwd_users where  site_code='1HZ') t_u  on  t.user_id=t_u.id
         |""".stripMargin

    val sql_dwd_transaction_types =
      """
        |insert into  dwd_transaction_types
        |select
        |'1HZ0'  site_code
        |,b.id  type_code
        |,b.cntitle type_name
        |,ifnull(p.paren_type_code,'')
        |,ifnull(p.paren_type_name,'')
        |,if(operations=1,operations,-1)
        |,now()
        |from
        |ods_1hz_ordertype  b
        |left  join  dwd_transaction_types_parent p on  b.site_code=p.site_code and b.id=p.type_code
        |""".stripMargin


    val sql_dwd_1hz_transaction_types =
      """
        |insert into  dwd_transaction_types
        |select
        |'1HZ'  site_code
        |,b.id  type_code
        |,b.cntitle type_name
        |,ifnull(p.paren_type_code,'')
        |,ifnull(p.paren_type_name,'')
        |,if(operations=1,operations,-1)
        |,now()
        |from
        |ods_1hz_ordertype  b
        |left  join  dwd_transaction_types_parent p on  b.site_code=p.site_code and b.id=p.type_code
        |""".stripMargin

    val sql_dwd_1hz_user_logins =
      s"""
         |insert  into  dwd_user_logins
         |SELECT date_sub(created_at, INTERVAL 6 HOUR) ,'1HZ' site_code,user_id,username,uuid,ip,location,client_type,is_first_login,is_agent,is_tester,parent_id,parent_username,user_chain_names,user_level,vip_level,is_vip,is_joint,user_created_at,date_sub(updated_at, INTERVAL 6 HOUR)
         |from  dwd_user_logins
         |where  site_code='1HZ0' and (created_at>='$startTime' and  created_at<= date_add('$endTime', INTERVAL 6 HOUR) )
         |""".stripMargin

    val sql_dwd_1hz_transactions =
      s"""
         |insert  into  dwd_transactions
         |SELECT date_sub(created_at, INTERVAL 6 HOUR) ,'1HZ' site_code,user_id,username,uuid,tran_no,project_no,trace_no,type_code,paren_type_code,paren_type_name,type_name,series_code,series_name,lottery_code,lottery_name,turnover_code,turnover_name,issue,issue_web,issue_date,amount,balance,is_first_deposit,is_first_withdraw,is_first_turnover,is_agent,is_tester,parent_id,parent_username,user_chain_names,user_level,vip_level,is_vip,is_joint,date_sub(user_created_at, INTERVAL 6 HOUR)   ,date_sub(updated_at, INTERVAL 6 HOUR)
         |from  dwd_transactions
         |where  site_code='1HZ0' and (created_at>='$startTime' and  created_at<= date_add('$endTime', INTERVAL 6 HOUR) )
         |""".stripMargin

    val sql_dwd_1hz_user_bank =
      s"""
         |insert  into  dwd_user_bank
         |select '1HZ' site_code,user_id,username,uuid,date_sub(created_at, INTERVAL 6 HOUR) ,bindcard_type,branch_name,is_agent,is_tester,parent_id,parent_username,user_chain_names,user_level,vip_level,is_vip,is_joint,date_sub(user_created_at, INTERVAL 6 HOUR) ,date_sub(updated_at, INTERVAL 6 HOUR)
         |from  dwd_user_bank
         |where  site_code='1HZ0' and (created_at>='$startTime' and  created_at<= date_add('$endTime', INTERVAL 6 HOUR) )
         |""".stripMargin

    val sql_del_dwd_1hz0_user_logins = s"delete from  dwd_user_logins  where   site_code in ('1HZ','1HZ0') and (created_at>='$startTime' and  created_at<='$endTime')"
    val sql_del_dwd_1hz0_transactions = s"delete from  dwd_transactions  where    site_code in ('1HZ','1HZ0') and (created_at>='$startTime' and  created_at<='$endTime')"
    val sql_del_dwd_1hz0_dwd_user_bank = s"delete from  dwd_user_bank  where   site_code in ('1HZ','1HZ0') and (created_at>='$startTime' and  created_at<='$endTime')"
    val start = System.currentTimeMillis()

    // 删除数据
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    if (isDeleteData) {
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, "", conn, "sql_del_dwd_1hz0_user_logins", sql_del_dwd_1hz0_user_logins)
      JdbcUtils.executeSiteDeletePartitionMonth(startTime, endTime, "", conn, "sql_del_dwd_1hz0_transactions", sql_del_dwd_1hz0_transactions)
      JdbcUtils.execute(conn, "sql_del_dwd_1hz0_dwd_user_bank", sql_del_dwd_1hz0_dwd_user_bank)
    }
    // 导入数据
    JdbcUtils.execute(conn, "sql_dwd_transaction_types", sql_dwd_transaction_types)
    JdbcUtils.execute(conn, "sql_dwd_1hz_transaction_types", sql_dwd_1hz_transaction_types)


    JdbcUtils.execute(conn, "sql_dwd_1hz0_user_logins", sql_dwd_1hz0_user_logins)
    JdbcUtils.execute(conn, "sql_dwd_1hz0_transactions", sql_dwd_1hz0_transactions)
    JdbcUtils.execute(conn, "sql_dwd_1hz0_user_bank", sql_dwd_1hz0_user_bank)

    JdbcUtils.execute(conn, "sql_dwd_1hz_user_logins", sql_dwd_1hz_user_logins)
    JdbcUtils.execute(conn, "sql_dwd_1hz_transactions", sql_dwd_1hz_transactions)
    JdbcUtils.execute(conn, "sql_dwd_1hz_user_bank", sql_dwd_1hz_user_bank)

    //    val map: Map[String, String] = Map(
    //      "sql_dwd_1hz0_user_logins" -> sql_dwd_1hz0_user_logins
    //      , "sql_dwd_1hz0_transactions" -> sql_dwd_1hz0_transactions
    //      , "sql_dwd_1hz0_user_bank" -> sql_dwd_1hz0_user_bank
    //    )
    //    ThreadPoolUtils.executeMap(map, conn, "doris_dt")
    //
    //    val map1hz: Map[String, String] = Map(
    //      "sql_dwd_1hz_user_logins" -> sql_dwd_1hz_user_logins
    //      , "sql_dwd_1hz_transactions" -> sql_dwd_1hz_transactions
    //      , "sql_dwd_1hz_user_bank" -> sql_dwd_1hz_user_bank
    //    )
    //    ThreadPoolUtils.executeMap(map1hz, conn, "doris_dt")
    val end = System.currentTimeMillis()
    logger.info("1HZ 数据归一累计耗时(毫秒):" + (end - start))
  }

  /**
   * 充值 提现 流程分析
   *
   * @param startTimeP
   * @param endTimeP
   * @param isDeleteData
   * @param conn
   */
  def runProcessData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = endTimeP
    val startDay = startTime.substring(0, 10)
    val endDay = endTime.substring(0, 10)
    logger.warn(s" --------------------- startTime : '$startDay'  , endTime '$endDay', isDeleteData '$isDeleteData'")

    val sql_dwd_deposit =
      s"""
         |insert  into  dwd_deposit
         |select
         |t.begindate
         |,'1HZ0'  site_code
         |,t.user_id
         |,t_u.username
         |,t.id
         |,t.orderno
         |,t.load_status status
         |,t.load_amount apply_amount
         |,t.updatedate   deposit_time
         |,null deposit_ip
         |,CASE t.from_where
         |WHEN 0   THEN  '未知'
         |WHEN 1   THEN  'web'
         |WHEN 2  THEN  '客户端'
         |WHEN 3  THEN  '手机端'
         |WHEN 4  THEN  'IOS'
         |WHEN 5  THEN  'ANDROID'
         |WHEN 6  THEN  '触屏'
         |ELSE concat( t.from_where,'_unKnow') END as  deposit_platfom
         |,null   deposit_channel
         |,CASE t.usefor
         |WHEN 1   THEN  '卡对卡'
         |WHEN 2  THEN  '第三方'
         |WHEN 3  THEN  '微信'
         |WHEN 4  THEN  '支付宝'
         |WHEN 5  THEN  '第三方卡对卡'
         |WHEN 6  THEN  'QQ'
         |WHEN 7  THEN  '快捷支付'
         |WHEN 8  THEN  '银联扫码'
         |WHEN 11  THEN  '支付宝到卡'
         |WHEN 12  THEN  '微信到卡'
         |WHEN 13  THEN  'USDT'
         |WHEN 14 THEN  'USDT_TRC'
         |ELSE concat( t.usefor,'_unKnow') END as  deposit_mode
         |,if(t.load_status=1,t.load_amount,0) deposit_amount
         |,(unix_timestamp(t.updatedate)- unix_timestamp(t.begindate) )  as  deposit_used_time
         |,if(t.usefor in (1),1,0) as  is_bind_card
         |,split_part(t_u.user_chain_names, '/', 2)  top_parent_username
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t.updatedate as updated_at
         |from
         |(
         |select  * from  ods_1hz_online_load  where (begindate>='$startTime' and  begindate<='$endTime')
         |) t
         |join   (select *  from doris_dt.dwd_users where  site_code='1HZ')  t_u  on t.site_code=t_u.site_code and  t.user_id=t_u.id
         |""".stripMargin

    val sql_dwd_withdraw =
      s"""
         |insert  into  dwd_withdraw
         |select
         |t.accepttime
         |,'1HZ0'  site_code
         |,t.userid user_id
         |,t_u.username
         |,t.entry id
         |,t.transno
         |,t.status status
         |,t.amount apply_amount
         |,t.v_time
         |,null appr2_time
         |,(unix_timestamp(t.v_time)- unix_timestamp(t.accepttime) )  as  appr_used_time
         |,t.pay_time   withdraw_time
         |,t.clientip  withdraw_ip
         |,null  withdraw_platfom
         |,null  withdraw_channel
         |,null  withdraw_mode
         |,if(t.status=2,t.amount-t.fee,0) withdraw_amount
         |,dealing_user_id
         |,dealing_user_name
         |,fee withdraw_fee
         |,if(pay_time='0000-00-00 00:00:00',0,(unix_timestamp(pay_time)- unix_timestamp(t.accepttime) ))   as withdraw_used_time
         |,split_part(t_u.user_chain_names, '/', 2)  top_parent_username
         |,t_u.is_agent
         |,t_u.is_tester
         |,t_u.parent_id
         |,t_u.parent_username
         |,t_u.user_chain_names
         |,t_u.user_level
         |,t.finishtime   updated_at
         |from
         |(
         |select  * from  ods_1hz_withdrawel   where (accepttime>='$startTime' and  accepttime<='$endTime')
         |) t
         |join   (select *  from  doris_dt.dwd_users where  site_code='1HZ')  t_u  on t.site_code=t_u.site_code and  t.userid=t_u.id
         |""".stripMargin

    val sql_dwd_deposit_1hz =
      s"""
         |insert  into dwd_deposit
         |select date_sub(apply_time, INTERVAL 6 HOUR) apply_time,'1HZ' site_code,user_id,username,id,sn,status,apply_amount,date_sub(deposit_time, INTERVAL 6 HOUR)  deposit_time,deposit_ip,deposit_platfom,deposit_channel,deposit_mode,deposit_amount,deposit_used_time,is_bind_card,top_parent_username,is_agent,is_tester,parent_id,parent_username,user_chain_names,user_level,date_sub(updated_at, INTERVAL 6 HOUR)  updated_at
         |from  dwd_deposit
         |where (apply_time>='$startTime' and  apply_time<='$endTime')
         |and site_code='1HZ0'
         |""".stripMargin

    val sql_dwd_withdraw_1hz =
      s"""
         |insert  into dwd_withdraw
         |select date_sub(apply_time, INTERVAL 6 HOUR)  apply_time,'1HZ' site_code,user_id,username,id,sn,status,apply_amount,date_sub(appr_time, INTERVAL 6 HOUR)   appr_time,date_sub(appr2_time, INTERVAL 6 HOUR) appr2_time,date_sub(appr_used_time, INTERVAL 6 HOUR) appr_used_time,date_sub(withdraw_time, INTERVAL 6 HOUR) withdraw_time,withdraw_ip,withdraw_platfom,withdraw_channel,withdraw_mode,withdraw_amount,auditor_id,auditor_name,withdraw_fee_amount,withdraw_used_time,top_parent_username,is_agent,is_tester,parent_id,parent_username,user_chain_names,user_level,date_sub(updated_at, INTERVAL 6 HOUR)   updated_at
         |from  dwd_withdraw
         |where (apply_time>='$startTime' and  apply_time<='$endTime')
         |and site_code='1HZ0'
         |""".stripMargin

    val start = System.currentTimeMillis()
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    JdbcUtils.execute(conn, "sql_dwd_deposit", sql_dwd_deposit)
    JdbcUtils.execute(conn, "sql_dwd_withdraw", sql_dwd_withdraw)

    JdbcUtils.execute(conn, "sql_dwd_deposit_1hz", sql_dwd_deposit_1hz)
    JdbcUtils.execute(conn, "sql_dwd_withdraw_1hz", sql_dwd_withdraw_1hz)

    val end = System.currentTimeMillis()
    logger.info("1HZ 充值提现流程 数据归一累计耗时(毫秒):" + (end - start))
  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_dt", "use doris_dt")
    runUserData("2010-12-20 00:00:00", "2023-12-20 00:00:00", true, conn)
    runData("2020-12-20 00:00:00", "2020-12-20 00:00:00", true, conn)
    JdbcUtils.close(conn)
  }
}
