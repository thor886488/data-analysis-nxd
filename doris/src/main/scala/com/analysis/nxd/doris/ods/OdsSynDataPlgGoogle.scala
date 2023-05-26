package com.analysis.nxd.doris.ods

import com.analysis.nxd.common.utils.{DateUtils, GoogleSheetsUtils, JdbcUtils}

import java.sql.Connection

object OdsSynDataPlgGoogle {
  def runData(startTimeP: String, endTimeP: String, isDeleteData: Boolean, dorisConn: Connection, mysqlConn: Connection): Unit = {
    val startTime = startTimeP
    val endTime = DateUtils.addSecond(endTimeP, 3600)

    JdbcUtils.execute(dorisConn, "use doris_dt", "use doris_dt")

    val listGatewayConf = GoogleSheetsUtils.readGoogleSheet("1G1k2mERIJAbPWMWytEapm2lfmDY1FFrztE_bhTsG3G0", "收款方式定义表!A2:C");
    JdbcUtils.executeList(dorisConn, mysqlConn, "ods_plg_gateway_conf", listGatewayConf)

    val listPlatformConf = GoogleSheetsUtils.readGoogleSheet("10ELAZqkPmrryHd3MUPui1_5e0CT72hDjGv_XdOUVFiI", "平台代号表!A2:D");
    JdbcUtils.executeList(dorisConn, mysqlConn, "ods_plg_platform_conf", listPlatformConf)

    val listChannelConf = GoogleSheetsUtils.readGoogleSheet("1P2FKRRHHv8AKbn22HEb-o4lMedbgIVin3KET86fifo8", "渠道代号表!A2:B");
    JdbcUtils.executeList(dorisConn, mysqlConn, "ods_plg_channel_conf", listChannelConf)

    val listChannelGatelConf = GoogleSheetsUtils.readGoogleSheet("1cK-bPWgP8CpxY_ghZ-P7RyVNVh0_7Yt-L7_dTZ0AhF4", "渠道信息!A2:E");
    JdbcUtils.executeList(dorisConn, mysqlConn, "ods_plg_channel_gate_conf", listChannelGatelConf)

    // 调整标准化报表_玩家
    //    val arrAdjustPlayer = Array(
    //      "1lcLiPUFkvXaGG7G9a_i-PfR8b8_-kednfLYJEe-qZnY",
    //      "1aorqfMhvjw2SVjla-hvL2HrSMJPEbWi0g4q46_gAoU0",
    //      "10N_HvjfcFEQuXHtbnXk_BmEyvIGcPaospxHW1yWASjA",
    //      "12CYqXBPU-5iUh0W3-qTxljgOLgxYB8dgrNLUqZsDndM",
    //      "13hdiTPmnfPKK8OQQgnhyHVvd9jKsLFbBJX8h1oUuqA8",
    //      "1QvelVqt_cWZStNfZjh72ejGfl0NIupvu3s_oJRbbP3Y")
    //    for (i <- 0 until arrAdjustPlayer.length) {
    //      val spreadsheetId = arrAdjustPlayer(i);
    //      val list = GoogleSheetsUtils.readGoogleSheet(spreadsheetId, "调整标准化报表玩家!A2:G", "platform,data_date,channel_code,type_code,type", "platform,data_date,channel_code,type_code,type,actual_amount,note", "actual_amount")
    //      JdbcUtils.executeList(conn, "ods_plg_adjust_player", list)
    //    }
    //
    //    //调整标准化报表非玩家
    //    val arrAdjustNoPlayer = Array(
    //      "1ukCrgghgOdErLb-X0IU6w3vnjuDcIah4U0hLPzrt3bA",
    //      "1RGK-SIYQ3MzEDNJcI-Ndfb-X207sUmtp5i_VvK0m8Q8",
    //      "1RTiqtKR9BpC1q99xKDf0Ucej-4PxhlbC-lXOsivahQo",
    //      "1lttWVQKOxpQRlcG9j2jnZlYRyS6hfe5wnQSo034h3AA",
    //      "17_gP6tjEcS9qrdb9tK0KbIKdmEFowvkJ2jpQzwYIhmQ",
    //      "1Qo7JQCwZ3cQWIK8yy8plWw_QpLtKTwjTIU9oY0Wcmsk")
    //    for (i <- 0 until arrAdjustNoPlayer.length) {
    //      val spreadsheetId = arrAdjustNoPlayer(i);
    //      val list = GoogleSheetsUtils.readGoogleSheet(spreadsheetId, "调整标准化报表非玩家!A2:G", "platform,data_date,channel_code,type_code,type", "platform,data_date,channel_code,type_code,type,actual_amount,note", "actual_amount")
    //      JdbcUtils.executeList(conn, "ods_plg_adjust_no_player", list)
    //    }
    //    // 渠道报表充值
    //    val arrChannelDeposit = Array("1hKuMhKN4foT4i2Y-dRhwgy7dSyv10FXhlYhs0pIUEDA",
    //      "12TVgizy6ciAj3jNlX-cxCmDVY7MnK-ofsobgLsH4X9o",
    //      "15qXqU3cFwN4jJ9M0Hm8sqq0cvRsMEaC7vzu5pW94oBM",
    //      "1hmWF8R0vfgSu6xaZGqqTI2nqZUhFHKXLoaLvlEX34Ec",
    //      "1xv4vDZTpTzQHzP2ac9u8V8IeK6K9iO0v41H-VJefOoM",
    //      "1fTN1ONcC8U7DRWT-kQch6p-3aAeTxYaKjrD1UYzKXgs",
    //      "1x-7nhEdqYgIfpdwipnFDkYwR4HGGgw5pDOz3FPMdfro",
    //      "13nU0oRQ_VDOUEapMF6oZxKOL6GynOvu8t2oJ5hRj8rw",
    //      "1j5Em24hC2FILC-1lYaIAIxlv4NPlI1bMpR2kioUarqs")
    //    for (i <- 0 until arrChannelDeposit.length) {
    //      val spreadsheetId = arrChannelDeposit(i);
    //      val list = GoogleSheetsUtils.readGoogleSheet(spreadsheetId, "渠道_充值!A2:AV", null, "date_time,platform,channel_code,channel_sn,platform_sn,type_code,type,status_code,status,amount_cny,fee_cny,platform_2,channel_name_2,user_id,real_name,UUID,created_at,Apply_amount,status_sn,complete_time,note,remark,accept_card_num,deposit_type,tag,operate,phone_num,confirm_time,order_amount,real_amount,deposit_fee,revise_amount,currency_type,channel,process_status,deposit_platfom,platform_name_1,platform_name_2,bank,chnnal_controlle,limit,limit_before,limit_after,process_time,rate,controlle_name,platform_controller", "amount_cny,fee_cny,order_amount,real_amount,deposit_fee,revise_amount")
    //      JdbcUtils.executeList(conn, "ods_plg_channel_deposit", list)
    //    }
    //    // 渠道报表提现
    //    val arrChannelWithdraw = Array("1KLE1XCcSc4ghaAwaoqaC4zn_wk8HNhcAYfj8laIJrxk",
    //      "10G1S6oPJ3Oo1JTiKH4l-S3AFvAaeCsUb_mSOy4PPHoY",
    //      "10L2HHfBWhXsfKKGz7poPZEMdkcQ10tqVt_wPHtVRKcI",
    //      "1_ReyScfdYlhBgqeSPF8MOJjl-aZNyP1yF05xdHREX7s",
    //      "1Y1qgbblD2hnmSSXKzid6AronssyGxalNnjY2kKRnXao",
    //      "1ulIlUFULFqKrX0zpIfplVVtzRCg87aLSRA-D7zp7kf4",
    //      "113n3DeXyQk2dxzI8qYtHR76lkd3vikW4tsHLw3eAKsE",
    //      "1blf10vuo9aAU-C8g07DnTL8Non9F2R9-zqbINWlLI1I",
    //      "1GxdgvrfwKgXM-Evofq0qYCxLQHhKiYLZE1NFWu7J554",
    //      "1l_yBSdSLbhJaFshXaI6aLcN3PBqDtjRqPPjEtui2RWU",
    //      "1SnPKXFU2CDFeQiadMnlBaALBGVA_FQbLzpzbawra98Q",
    //      "1oa7tgxYQHY2oBwZUZBUdyyVZPTXuk0GRebXyebjAjh8",
    //      "1iPZBUz1GDgMT4HwLdJI-0ZgGVzkJfAson3iiLRQVbog",
    //      "1noJkijV_RS5lNTpj9jvJkR0pO5DEc7F7KaDXHkCydr4",
    //      "1zFGFJbEUQdaMyrmuAqW2O7niBAMBlLOgvlryI-2Ei9c",
    //      "1vGE2K2sk-mwcmmouU72PcSjqoD4qESY1ZN4IzqNh0ns",
    //      "1_KOydyvr7LZsrdSJqYJ8NrwlTj-7QSmGYNa-kI8Y5S8",
    //      "1wweHg9f24nBuu00DbB3UxQ5_DiLkWNKQXTuKKDBHo-I",
    //      "1JvmEgcD2jqoza9PKU0HpZW02Q6t-wEheZU1MnEcYDnk",
    //      "1C011B8KmkcSSUu6R5Jct_IquAChaqSaGS4rWGlAFv2k")
    //    for (i <- 0 until arrChannelWithdraw.length) {
    //      val spreadsheetId = arrChannelWithdraw(i);
    //      val list = GoogleSheetsUtils.readGoogleSheet(spreadsheetId, "渠道后台标准化报表_提现!A2:T");
    //      JdbcUtils.executeList(conn, "ods_plg_channel_withdraw", list)
    //    }
    //
    //    // 渠道报表 充值 提现
    //    val arrChannelDepositWithdraw = Array("1TWrgpGq7p0yEFFQCMoanJ90zIMpge_PQs9-4QTuGS38",
    //      "1BD9twKi1T3eMbNNhqepR63uw3o4QDap3Gtnpi9BmC70",
    //      "1SnP3jmkCrATOXI7yFDT_yOXBxp3pxVF5Ts7HxTmIkXM",
    //      "1Rf6UcoCK9NlDQoVq8jToDE_nNRjosDkkrAcA7QefFRU",
    //      "1shzbZQyc7F-PJPoLSt4WaKY-mwJYnE-fI-s8TQkjZes",
    //      "1ejapxZNLpmvhBDQuQ6YIREK0hvlDLh_gc2tsgiYROR8",
    //      "1t_phm0HcxmNMPKIuDVKcpcKD-VPkJ_UCe9O_RLD8z8c",
    //      "148JcNuxbx1xamXNEOjgodd5oaLLCicmwqcZuZLt9AqE",
    //      "1-qrtaHIg1dQfB-7GSP3FTr3WfeB-sIHI5JmXunymcbU",
    //      "1niVSXfvkd3rXZIKBPAockkG_ir-4py0WvfSLcLOmK7U",
    //      "1y23IgDdLe3rOZTAx_HG86bw4Isyej6pEV_BCXi8xI6Y",
    //      "1dxrx0_6QPe8G1YUIboRfPfgt4KMg_i6hbWcrjNBouRc",
    //      "1C9bxh1xOKUxU5SuTub7HNKuz1SwZnxMn01R35zuyXcQ",
    //      "1kKk8Zx57uJO781ZrihRJOgHe4DP8epM7QcpaAlnEf3k",
    //      "1NFJqwr9TA-4y57YUiU2XqxbcbPZzoEBl5PeMG_fNoWg",
    //      "1xRo6qyi44qv6RH-nfnOqlAot6jquML8TLrREmufmK1E",
    //      "1bt5EIWdm4unp0p2zJB44gUK7uU7-3NkIVf5CgRCnXm8",
    //      "1HLkQt9M8ZEompta2b1gs55eO77xvWu-vn8ztnhMj98U",
    //      "1QIhvazuv74fHmejS6PndzxAEOQ0oEdqNNg4GswgpTag",
    //      "1ezda7eavTFQhN1Wif3cT8ZU0boFk203XcGM7Pm-7RC0")
    //    for (i <- 0 until arrChannelDepositWithdraw.length) {
    //      val spreadsheetId = arrChannelDepositWithdraw(i);
    //      val list = GoogleSheetsUtils.readGoogleSheetSpecific(spreadsheetId, "渠道后台标准化报表_充值_提现!A2:M");
    //      JdbcUtils.executeList(conn, "ods_plg_channel_deposit_withdraw", list)
    //    }
    //
    //
    //    val sql_ods_plg_channel_deposit_withdraw =
    //      s"""
    //         |insert into  ods_plg_channel
    //         |select  order_time,platform,channel,null sn,order_num,type,status,status_code,actual_amount,fee_amount,updated_at  from
    //         |ods_plg_channel_deposit_withdraw
    //         |where   (order_time>='$startTime' and  order_time<='$endTime')
    //         |""".stripMargin
    //
    //    val sql_ods_plg_channel_deposit =
    //      s"""
    //         |insert into  ods_plg_channel
    //         |select  order_time,platform,channel,sn,order_num,type,status,status_code,actual_amount,fee_amount,updated_at  from
    //         |ods_plg_channel_deposit
    //         |where   (order_time>='$startTime' and  order_time<='$endTime')
    //         |""".stripMargin
    //
    //    val sql_ods_plg_channel_withdraw =
    //      s"""
    //         |insert into  ods_plg_channel
    //         |select  order_time,platform,channel,sn,order_num,type,status,status_code,actual_amount,fee_amount,updated_at  from
    //         |ods_plg_channel_withdraw
    //         |where   (order_time>='$startTime' and  order_time<='$endTime')
    //         |""".stripMargin
    //
    //    JdbcUtils.execute(conn, "sql_ods_plg_channel_deposit_withdraw", sql_ods_plg_channel_deposit_withdraw)
    //    JdbcUtils.execute(conn, "sql_ods_plg_channel_deposit", sql_ods_plg_channel_deposit)
    //    JdbcUtils.execute(conn, "sql_ods_plg_channel_withdraw", sql_ods_plg_channel_withdraw)

  }


  def main(args: Array[String]): Unit = {
    // 渠道报表充值
    // 渠道报表充值
    val arrChannelDeposit = Array("1hKuMhKN4foT4i2Y-dRhwgy7dSyv10FXhlYhs0pIUEDA",
      "12TVgizy6ciAj3jNlX-cxCmDVY7MnK-ofsobgLsH4X9o",
      "15qXqU3cFwN4jJ9M0Hm8sqq0cvRsMEaC7vzu5pW94oBM",
      "1hmWF8R0vfgSu6xaZGqqTI2nqZUhFHKXLoaLvlEX34Ec",
      "1xv4vDZTpTzQHzP2ac9u8V8IeK6K9iO0v41H-VJefOoM",
      "1fTN1ONcC8U7DRWT-kQch6p-3aAeTxYaKjrD1UYzKXgs",
      "1x-7nhEdqYgIfpdwipnFDkYwR4HGGgw5pDOz3FPMdfro",
      "13nU0oRQ_VDOUEapMF6oZxKOL6GynOvu8t2oJ5hRj8rw",
      "1j5Em24hC2FILC-1lYaIAIxlv4NPlI1bMpR2kioUarqs")
    for (i <- 0 until arrChannelDeposit.length) {
      val spreadsheetId = arrChannelDeposit(i);
      val list = GoogleSheetsUtils.readGoogleSheet(spreadsheetId, "渠道_充值!A2:AV", null, "date_time,platform,channel_code,channel_sn,platform_sn,type_code,type,status_code,status,amount_cny,fee_cny,platform_2,channel_name_2,user_id,real_name,UUID,created_at,Apply_amount,status_sn,complete_time,note,remark,accept_card_num,deposit_type,tag,operate,phone_num,confirm_time,order_amount,real_amount,deposit_fee,revise_amount,currency_type,channel,process_status,deposit_platfom,platform_name_1,platform_name_2,bank,chnnal_controlle,limit,limit_before,limit_after,process_time,rate,controlle_name,platform_controller", "amount_cny,fee_cny,order_amount,real_amount,deposit_fee,revise_amount")
    }
  }
}