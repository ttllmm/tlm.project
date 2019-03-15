package cn.tedu

import java.sql.DriverManager
import java.sql.Date

object Demo01 {
  def main(args: Array[String]): Unit = {
    //1.注册数据库驱动
    Class.forName("com.mysql.jdbc.Driver");
    //2.获取数据库连接
    val conn = DriverManager.getConnection("jdbc:mysql:///fluxdb","root","root")
    //3.获取传输器
    val ps = conn.prepareStatement("insert into tongji2 values (?,?,?,?,?,?)")
    ps.setDate(1, new Date(1552460287622l))
    ps.setInt(2, 1);
    ps.setInt(3, 1);
    ps.setInt(4, 1);
    ps.setInt(5, 1);
    ps.setInt(6, 1);
    //4.传输sql执行得到结果
    ps.executeUpdate()
    //5.处理结果
    
    //6.关闭连接
    ps.close()
    conn.close()
  }
}