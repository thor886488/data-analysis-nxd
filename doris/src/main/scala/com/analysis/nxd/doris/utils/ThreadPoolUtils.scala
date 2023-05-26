package com.analysis.nxd.doris.utils

import com.analysis.nxd.common.utils.JdbcUtils

import java.sql.Connection
import java.util.concurrent.{CountDownLatch, ExecutorService, Executors, TimeUnit}

object ThreadPoolUtils {

  /**
   * 传入 conn ，效率蛮慢 ， 占用数据库资源少
   *
   * @param map
   * @param dataBase
   */
  def executeMap1HZ(map: Map[String, String], conn: Connection, dataBase: String) {
    val threadPool: ExecutorService = Executors.newFixedThreadPool(map.size)
    val countDownLatch = new CountDownLatch(map.size)
    JdbcUtils.execute(conn, "use " + dataBase, "use " + dataBase)
    map.keys.foreach { label =>
      val sql = map(label)
      threadPool.execute(new Runnable {
        override def run(): Unit = {
          try {
            JdbcUtils.executeSyn1HZ(conn, label, sql)
          } finally {
            countDownLatch.countDown()
          }
        }
      })
    }
    try {
      countDownLatch.await(3600, TimeUnit.SECONDS)
    } finally {
      threadPool.shutdown()
    }
  }

  /**
   * 传入 conn ，效率蛮慢 ， 占用数据库资源少
   *
   * @param map
   * @param dataBase
   */
  def executeMap(map: Map[String, String], conn: Connection, dataBase: String) {
    val threadPool: ExecutorService = Executors.newFixedThreadPool(map.size)
    val countDownLatch = new CountDownLatch(map.size)
    JdbcUtils.execute(conn, "use " + dataBase, "use " + dataBase)
    map.keys.foreach { label =>
      val sql = map(label)
      threadPool.execute(new Runnable {
        override def run(): Unit = {
          try {
            JdbcUtils.execute(conn, label, sql)
          } finally {
            countDownLatch.countDown()
          }
        }
      })
    }
    try {
      countDownLatch.await(3600, TimeUnit.SECONDS)
    } finally {
      threadPool.shutdown()
    }
  }

  /**
   * 传入 conn ，效率蛮慢 ， 占用数据库资源少
   *
   * @param map
   * @param dataBase
   */
  def executeSiteMap(siteCode: String, map: Map[String, String], conn: Connection, dataBase: String) {
    val threadPool: ExecutorService = Executors.newFixedThreadPool(map.size)
    val countDownLatch = new CountDownLatch(map.size)
    JdbcUtils.execute(conn, "use " + dataBase, "use " + dataBase)
    map.keys.foreach { label =>
      val sql = map(label)
      threadPool.execute(new Runnable {
        override def run(): Unit = {
          try {
            JdbcUtils.executeSite(siteCode, conn, label, sql)
          } finally {
            countDownLatch.countDown()
          }
        }
      })
    }
    try {
      countDownLatch.await(3600, TimeUnit.SECONDS)
    } finally {
      threadPool.shutdown()
    }
  }
}
