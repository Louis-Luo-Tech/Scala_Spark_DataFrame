package com.louis.bigdata

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

object JDBCClientApp {
  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val conn: Connection = DriverManager.getConnection("jdbc:hive2://localhost:10000")
    val statement: PreparedStatement = conn.prepareStatement("select * from pk")
    val set: ResultSet = statement.executeQuery()
    while(set.next()){
      println(set.getObject(1) + ":" + set.getObject(2))
    }
  }
}
