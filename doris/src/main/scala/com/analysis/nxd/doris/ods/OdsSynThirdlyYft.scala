package com.analysis.nxd.doris.ods

import java.sql.Connection
import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.utils.{ThreadPoolUtils, VerifyDataUtils}
import org.slf4j.LoggerFactory

object OdsSynThirdlyYft {

  val logger = LoggerFactory.getLogger(OdsSynThirdlyYft.getClass)

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = DateUtils.addSecond(startTimeP, -3600 * 12)
    val endTime = endTimeP + ".999"
    val sql_ods_y_gaming_bet_record =
      s"""
         |insert into  ods_yft_gaming_bet_record
         |select  create_date,'Y' site_code,third_platform_type,game_type,data_type,uid,order_no,bet_amount,valid_bet_amount,net_amount,game_category,player_ip_addr
         |from  syn_y_gaming_bet_record
         |where (create_date>='$startTime' and  create_date<='$endTime')
         |""".stripMargin
    val sql_ods_t_gaming_bet_record =
      s"""
         |insert into  ods_yft_gaming_bet_record
         |select  create_date,'T' site_code,third_platform_type,game_type,data_type,uid,order_no,bet_amount,valid_bet_amount,net_amount,game_category,player_ip_addr
         |from  syn_t_gaming_bet_record
         |where (create_date>='$startTime' and  create_date<='$endTime')
         |""".stripMargin
    val sql_ods_y_game_order_record =
      s"""
         |insert into  ods_yft_game_order_record
         |select create_date,'Y' site_code,game_type,uid,order_no,transfer_type,order_amount,bill_no
         |from  syn_y_game_order_record
         |where (create_date>='$startTime' and  create_date<='$endTime')
         |""".stripMargin
    val sql_ods_t_game_order_record =
      s"""
         |insert into  ods_yft_game_order_record
         |select create_date,'T' site_code,game_type,uid,order_no,transfer_type,order_amount,bill_no
         |from  syn_t_game_order_record
         |where (create_date>='$startTime' and  create_date<='$endTime')
         |""".stripMargin
    val sql_ods_yft_third_game_category =
      s"""
         |insert into ods_yft_third_game_category
         |select   game_platform,game_code,game_category,game_name from  syn_yft_third_game_category
         |""".stripMargin
    val sql_ods_yft_ag_bet_record =
      s"""
         |insert into  ods_yft_ag_bet_record
         |select  bet_time,'YFT' site_code,third_platform_type,billno,game_type,player_name,data_type,main_billno,bet_amount,valid_bet_amount,net_amount,currency,game_category,player_ip_addr,recalcu_time,ftp_file_full_name
         |from  syn_yft_ag_bet_record
         |where (bet_time>='$startTime' and  bet_time<='$endTime')
         |""".stripMargin
    val sql_ods_yft_ag_fish_hunter =
      s"""
         |insert into  ods_yft_ag_fish_hunter
         |select  bet_time,'YFT' site_code,third_platform_type,trade_no,game_type,player_name,data_type,transfer_amount,cost_amount,earn_amount,currency,game_category,player_ip_addr,ftp_file_full_name
         |from  syn_yft_ag_fish_hunter
         |where (bet_time>='$startTime' and  bet_time<='$endTime')
         |""".stripMargin
    val sql_ods_yft_ky_bet_record=
      s"""
         |insert into  ods_yft_ky_bet_record
         |select  create_date,'YFT' site_code,billno,player_name,server_id,kind_id,table_id,chair_id,user_count,card_value,cell_score,all_bet,game_start_time,game_end_time,channel_id,line_code
         |from  syn_yft_ky_bet_record
         |where (create_date>='$startTime' and  create_date<='$endTime')
         |""".stripMargin
    val sql_ods_yft_third_bet_record =
      s"""
         |insert into  ods_yft_third_bet_record
         |select  create_time,'YFT' site_code,third_platform_type,billno,player_name,data_type,game_type,main_billno,bet_amount,valid_bet_amount,net_amount,currency,game_category,player_ip_addr,bet_time,recalcu_time,resolved,update_time
         |from  syn_yft_third_bet_record
         |where (create_time>='$startTime' and  create_time<='$endTime')
         |""".stripMargin
    val sql_ods_y_promo_gaming_daily_record =
      s"""
         |insert into  ods_yft_promo_gaming_daily_record
         |select  date_add(create_date,interval 5 hour) as create_date,'Y' site_code,uid,report_date,record_no,live_bet_amount,sports_bet_amount,elec_bet_amount,live_rackback_amount,sports_rackback_amount,elec_rackback_amount,attr_1,attr_2,attr_3,attr_4,attr_5,attr_6,attr_7,update_date
         |from  syn_pg_y_promo_gaming_daily_record
         |where (create_date>='$startTime' and  create_date<='$endTime')
         |""".stripMargin
    val sql_ods_t_promo_gaming_daily_record =
      s"""
         |insert into  ods_yft_promo_gaming_daily_record
         |select  date_add(create_date,interval 5 hour) as create_date,'T' site_code,uid,report_date,record_no,live_bet_amount,sports_bet_amount,elec_bet_amount,live_rackback_amount,sports_rackback_amount,elec_rackback_amount,attr_1,attr_2,attr_3,attr_4,attr_5,attr_6,attr_7,update_date
         |from  syn_pg_t_promo_gaming_daily_record
         |where (create_date>='$startTime' and  create_date<='$endTime')
         |""".stripMargin

    val start = System.currentTimeMillis()
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    JdbcUtils.execute(conn, "sql_ods_y_gaming_bet_record", sql_ods_y_gaming_bet_record)
    JdbcUtils.execute(conn, "sql_ods_t_gaming_bet_record", sql_ods_t_gaming_bet_record)
    JdbcUtils.execute(conn, "sql_ods_y_game_order_record", sql_ods_y_game_order_record)
    JdbcUtils.execute(conn, "sql_ods_t_game_order_record", sql_ods_t_game_order_record)
    JdbcUtils.execute(conn, "sql_ods_yft_third_game_category", sql_ods_yft_third_game_category)
    JdbcUtils.execute(conn, "sql_ods_yft_ag_bet_record", sql_ods_yft_ag_bet_record)
    JdbcUtils.execute(conn, "sql_ods_yft_ag_fish_hunter", sql_ods_yft_ag_fish_hunter)
    JdbcUtils.execute(conn, "sql_ods_yft_ky_bet_record", sql_ods_yft_ky_bet_record)
    JdbcUtils.execute(conn, "sql_ods_yft_third_bet_record", sql_ods_yft_third_bet_record)
    JdbcUtils.execute(conn, "sql_ods_y_promo_gaming_daily_record", sql_ods_y_promo_gaming_daily_record)
    JdbcUtils.execute(conn, "sql_ods_t_promo_gaming_daily_record", sql_ods_t_promo_gaming_daily_record)
    val end = System.currentTimeMillis()
    logger.info("YFT站 三方数据同步累计耗时(毫秒):" + (end - start))
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
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    val sql_syn_y_gaming_bet_record_count = s"select   count(1) countData  from syn_y_gaming_bet_record where create_date>='$startTime' and create_date < '$endTime'"
    val sql_ods_y_gaming_bet_record_count = s"select   count(1) countData  from ods_yft_gaming_bet_record where site_code='Y' and bet_time>='$startTime' and bet_time < '$endTime' "
    VerifyDataUtils.verifyData("sql_ods_y_gaming_bet_record_count", sql_syn_y_gaming_bet_record_count, sql_ods_y_gaming_bet_record_count, conn)

    val sql_syn_t_gaming_bet_record_count = s"select   count(1) countData  from syn_t_gaming_bet_record where create_date>='$startTime' and create_date < '$endTime'"
    val sql_ods_t_gaming_bet_record_count = s"select   count(1) countData  from ods_yft_gaming_bet_record where site_code='T' and bet_time>='$startTime' and bet_time < '$endTime' "
    VerifyDataUtils.verifyData("sql_ods_t_gaming_bet_record_count", sql_syn_t_gaming_bet_record_count, sql_ods_t_gaming_bet_record_count, conn)

    val sql_syn_y_game_order_record_count = s"select   count(1) countData  from syn_y_game_order_record where create_date>='$startTime' and create_date < '$endTime'"
    val sql_ods_y_game_order_record_count = s"select   count(1) countData  from ods_yft_game_order_record where site_code='Y' and create_date>='$startTime' and create_date < '$endTime' "
    VerifyDataUtils.verifyData("sql_ods_y_game_order_record_count", sql_syn_y_game_order_record_count, sql_ods_y_game_order_record_count, conn)

    val sql_syn_t_game_order_record_count = s"select   count(1) countData  from syn_t_game_order_record where create_date>='$startTime' and create_date < '$endTime'"
    val sql_ods_t_game_order_record_count = s"select   count(1) countData  from ods_yft_game_order_record where site_code='T' and create_date>='$startTime' and create_date < '$endTime' "
    VerifyDataUtils.verifyData("sql_ods_t_game_order_record_count", sql_syn_t_game_order_record_count, sql_ods_t_game_order_record_count, conn)

    val sql_syn_yft_ag_bet_record = s"select   count(1) countData  from syn_yft_ag_bet_record where bet_time>='$startTime' and bet_time < '$endTime'"
    val sql_ods_yft_ag_bet_record = s"select   count(1) countData  from ods_yft_ag_bet_record where site_code='YFT' and bet_time>='$startTime' and bet_time < '$endTime' "
    VerifyDataUtils.verifyData("sql_ods_yft_ag_bet_record", sql_syn_yft_ag_bet_record, sql_ods_yft_ag_bet_record, conn)

    val sql_syn_yft_ag_fish_hunter = s"select   count(1) countData  from syn_yft_ag_fish_hunter where bet_time>='$startTime' and bet_time < '$endTime'"
    val sql_ods_yft_ag_fish_hunter = s"select   count(1) countData  from ods_yft_ag_fish_hunter where site_code='YFT' and bet_time>='$startTime' and bet_time < '$endTime' "
    VerifyDataUtils.verifyData("sql_ods_ag_fish_hunter", sql_syn_yft_ag_fish_hunter, sql_ods_yft_ag_fish_hunter, conn)

    val sql_syn_yft_ky_bet_record = s"select   count(1) countData  from syn_yft_ky_bet_record where create_date>='$startTime' and create_date < '$endTime'"
    val sql_ods_yft_ky_bet_record = s"select   count(1) countData  from ods_yft_ky_bet_record where site_code='YFT' and create_date>='$startTime' and create_date < '$endTime' "
    VerifyDataUtils.verifyData("sql_ods_ky_bet_record", sql_syn_yft_ky_bet_record, sql_ods_yft_ky_bet_record, conn)

    val sql_syn_yft_third_bet_record = s"select   count(1) countData  from syn_yft_third_bet_record where create_time>='$startTime' and create_time < '$endTime'"
    val sql_ods_yft_third_bet_record = s"select   count(1) countData  from ods_yft_third_bet_record where site_code='YFT' and create_time>='$startTime' and create_time < '$endTime' "
    VerifyDataUtils.verifyData("sql_ods_third_bet_record", sql_syn_yft_third_bet_record, sql_ods_yft_third_bet_record, conn)

    val sql_syn_y_promo_gaming_daily_record = s"select   count(1) countData  from syn_y_promo_gaming_daily_record where (create_date>=date_add('$startTime',INTERVAL 5 HOUR) and  create_date<date_add('$endTime',INTERVAL 5 HOUR))"
    val sql_ods_y_promo_gaming_daily_record = s"select   count(1) countData  from ods_yft_promo_gaming_daily_record where site_code='Y' and (create_date>=date_add('$startTime',INTERVAL 5 HOUR) and  create_date<=date_add('$endTime',INTERVAL 5 HOUR)) "
    VerifyDataUtils.verifyData("sql_ods_y_gaming_bet_record_count", sql_syn_y_promo_gaming_daily_record, sql_ods_y_promo_gaming_daily_record, conn)

    val sql_syn_t_promo_gaming_daily_record = s"select   count(1) countData  from syn_t_promo_gaming_daily_record where (create_date>=date_add('$startTime',INTERVAL 5 HOUR) and  create_date<=date_add('$endTime',INTERVAL 5 HOUR))"
    val sql_ods_t_promo_gaming_daily_record = s"select   count(1) countData  from ods_yft_promo_gaming_daily_record where site_code='T' and (create_date>=date_add('$startTime',INTERVAL 5 HOUR) and  create_date<date_add('$endTime',INTERVAL 5 HOUR)) "
    VerifyDataUtils.verifyData("sql_ods_t_gaming_bet_record_count", sql_syn_t_promo_gaming_daily_record, sql_ods_t_promo_gaming_daily_record, conn)

  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.execute(conn, "use doris_thirdly", "use doris_thirdly")
    runData("2021-05-02 00:00:00", "2020-12-30 00:00:00", false, conn)
    JdbcUtils.close(conn)
  }
}
