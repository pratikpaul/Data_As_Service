package com.self.dataAsService

import java.sql.Connection
import javax.naming.InitialContext
import javax.sql.DataSource
import scalikejdbc.AutoSession
import scalikejdbc.ConnectionPool
import com.typesafe.config.ConfigFactory

object DatabaseConn {
  val conf = ConfigFactory.load
  val driver = conf.getString("mySQLDetail.driver") //"jdbc:mysql://CentOS7-1:3306/daas"
  val hive_base_driver = conf.getString("hiveDetail.base_driver")
  val hive_driver = conf.getString("hiveDetail.driver") //"jdbc:hive2://198.168.0.5:10000/demo"
  val hive_user_name = conf.getString("hiveDetail.user_name")
  val hive_password = conf.getString("hiveDetail.password")
  //  val driver = "jdbc:mysql://198.168.0.31:3306/daas"
  //  val db_user_name = "dmf | root"
  val db_user_name = conf.getString("mySQLDetail.user_name")
  //  val db_password = "dmf2016 | 6yhnZAQ!"
  val db_password = conf.getString("mySQLDetail.password")
  
  val dataManipulationJdbcUrl = conf.getString("DataManipulation.jdbcUrl")
  val eihDataSchema = conf.getString("DataManipulation.eihDataSchema")
  val appDataSchema = conf.getString("DataManipulation.appDataSchema")

  Class.forName("com.mysql.jdbc.Driver")
  ConnectionPool.singleton(driver, db_user_name, db_password)

  implicit val session = AutoSession
}