package com.analysis.nxd.doris.ods

import com.analysis.nxd.common.utils.{DateUtils, JdbcUtils}
import com.analysis.nxd.doris.utils.VerifyDataUtils
import org.slf4j.LoggerFactory

import java.sql.Connection

object OdsSynThirdly1HZ {
  val logger = LoggerFactory.getLogger(OdsSynThirdlyBM.getClass)

  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, conn: Connection): Unit = {
    val startTime = DateUtils.addSecond(startTimeP, -3600 * 12)
    val endTime = endTimeP
    val sql_ods_1hz_game_records_ag =
      s"""
         |insert  into  ods_1hz_game_records_ag
         |select recalcuTime,'1HZ' site_code,'AG' thirdly_code,id,dataType,billNo,playerName,agentCode,gameCode,netAmount,betTime,gameType,betAmount,validBetAmount,flag,playType,currency,tableCode,loginIP,platformType,remark,`round`,`result`,beforeCredit,created_at,updated_at,terminal_id
         |from  syn_1hz_game_records_ag
         |where (recalcuTime>='$startTime' and  recalcuTime<='$endTime')
         |""".stripMargin
    val sql_ods_1hz_game_records_lc =
      s"""
         |insert  into  ods_1hz_game_records_lc
         |select GameStartTime game_start_time ,'1HZ' site_code,'LC' thirdly_code,id,GameID game_id, Account account, ServerID server_id,KindID kind_id,TableID table_id,ChairID chair_id,UserCount user_count,CardValue card_value,CellScore cell_score,AllBet all_bet,Profit profit,Revenue revenue,GameEndTime game_end_time,ChannelID channel_id,LineCode line_code,now() created_at,now()  updated_at,0 terminal_id
         |from  syn_1hz_game_records_lc
         |where (GameStartTime>='$startTime' and  GameStartTime<='$endTime')
         |""".stripMargin
    val sql_ods_1hz_game_records_shaba =
      s"""
         |insert  into ods_1hz_game_records_shaba
         |select transaction_time,'1HZ' site_code,'SHABA' thirdly_code,id,type_string,vendor_member_id,account,operator_id,trans_id,ticket_status,bet_type,bet_team,stake,winlost_amount,after_amount,award,CashOutData,original_stake,terminal_id,created_at,updated_at
         |from  syn_1hz_game_records_shaba
         |where (transaction_time>='$startTime' and  transaction_time<='$endTime')
         |""".stripMargin

    val sql_ods_1hz_game_records_gemini =
      s"""
         |insert  into ods_1hz_game_records_gemini
         |select id,'1HZ' site_code,'GEMINI'  thirdly_code,bill_no,account,game_type,game_code,group_type,bet_type,bet_amount,item_amount,turnover,won_amount,win_lose,bill_status,bet_time,reckon_time,play_type,currency,created_at,updated_at
         |from  syn_1hz_game_records_gemini
         |where (reckon_time>='$startTime' and  reckon_time<='$endTime')
         |""".stripMargin

    JdbcUtils.executeSyn1HZ(conn, "use doris_thirdly", "use doris_thirdly")
    val start = System.currentTimeMillis()
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_game_records_ag", sql_ods_1hz_game_records_ag)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_game_records_lc", sql_ods_1hz_game_records_lc)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_game_records_shaba", sql_ods_1hz_game_records_shaba)
    JdbcUtils.executeSyn1HZ(conn, "sql_ods_1hz_game_records_gemini", sql_ods_1hz_game_records_gemini)

    //    val map: Map[String, String] = Map(
    //      "sql_ods_1hz_game_records_ag" -> sql_ods_1hz_game_records_ag
    //      , "sql_ods_1hz_game_records_lc" -> sql_ods_1hz_game_records_lc
    //      , "sql_ods_1hz_game_records_shaba" -> sql_ods_1hz_game_records_shaba
    //    )
    //    ThreadPoolUtils.executeMap1HZ(map, conn, "doris_thirdly")


    val end = System.currentTimeMillis()
    logger.info("1HZ站 三方数据同步累计耗时(毫秒):" + (end - start))

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

    val sql_syn_1hz_game_records_ag_count = s"select   count(1) countData  from syn_1hz_game_records_ag where  recalcuTime>='$startTime' and recalcuTime<='$endTime'"
    val sql_ods_1hz_game_records_ag_count = s"select   count(1) countData  from ods_1hz_game_records_ag where  recalcuTime>='$startTime' and recalcuTime<='$endTime'"
    VerifyDataUtils.verifyData1HZ("sql_ods_1hz_game_records_ag_count", sql_syn_1hz_game_records_ag_count, sql_ods_1hz_game_records_ag_count, conn)

    val sql_syn_1hz_game_records_lc_count = s"select   count(1) countData  from syn_1hz_game_records_lc where  GameStartTime>='$startTime' and GameStartTime<='$endTime'"
    val sql_ods_1hz_game_records_lc_count = s"select   count(1) countData  from ods_1hz_game_records_lc where  game_start_time>='$startTime' and game_start_time<='$endTime'"
    VerifyDataUtils.verifyData1HZ("sql_ods_1hz_game_records_lc_count", sql_syn_1hz_game_records_lc_count, sql_ods_1hz_game_records_lc_count, conn)


    val sql_syn_1hz_game_records_shaba_count = s"select   count(1) countData  from syn_1hz_game_records_shaba where  transaction_time>='$startTime' and transaction_time<='$endTime'"
    val sql_ods_1hz_game_records_shaba_count = s"select   count(1) countData  from ods_1hz_game_records_shaba where  transaction_time>='$startTime' and transaction_time<='$endTime'"
    VerifyDataUtils.verifyData1HZ("sql_ods_1hz_game_records_shaba_count", sql_syn_1hz_game_records_shaba_count, sql_ods_1hz_game_records_shaba_count, conn)
  }

  def main(args: Array[String]): Unit = {
    val conn = JdbcUtils.getConnection()
    JdbcUtils.executeSyn1HZ(conn, "use doris_thirdly", "use doris_thirdly")
    runData("2021-05-02 00:00:00", "2020-12-30 00:00:00", false, conn)
    JdbcUtils.close(conn)
  }

}
