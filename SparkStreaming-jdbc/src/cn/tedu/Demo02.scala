package cn.tedu

import java.sql.DriverManager
import java.sql.Date

object Demo02 {
  def main(args: Array[String]): Unit = {
    //1.注册数据库驱动
    Class.forName("com.mysql.jdbc.Driver");
    //2.获取数据库连接
    val conn = DriverManager.getConnection("jdbc:mysql:///fluxdb","root","root")
    //3.获取传输器
    val ps = conn.prepareStatement("select * from tongji2 where reportTime = ?")
    ps.setDate(1, new Date(1552460287622l))
    //4.传输sql执行得到结果
    val rs = ps.executeQuery()
    //5.处理结果
    while(rs.next()){
      val time = rs.getDate("reportTime");
      println(time)
    }
    //6.关闭连接
    rs.close()
    ps.close()
    conn.close()
  }
}