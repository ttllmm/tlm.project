package cn.tedu

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes

object Demo002 {
  def main(args: Array[String]): Unit = {
    //1.创建sc
    val conf = new SparkConf();
    conf.setAppName("Spark_Hbase")
    conf.setMaster("local");
    val sc = new SparkContext(conf);

    //2.指定Hbase配置对象，指定hbase基本配置信息
    val confx = HBaseConfiguration.create()
    confx.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03")
    confx.set("hbase.zookeeper.property.clientPort", "2181")
    confx.set(TableInputFormat.INPUT_TABLE, "tab1")

    //3.读取hbase
    val hBaseRDD = sc.newAPIHadoopRDD(confx, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //4.查看数据量
    val count = hBaseRDD.count()
    println(count)

    //5.遍历数据
    hBaseRDD.foreach {
      case (_, result) => {
        //获取行键  
        val key = Bytes.toString(result.getRow)
        //通过列族和列名获取列  
        val name = Bytes.toString(result.getValue("cf1".getBytes, "name".getBytes))
        val age = Bytes.toString(result.getValue("cf1".getBytes, "age".getBytes))
        println("Row key:" + key + " name:" + name + " age:" + age)
      }
    }
  }
}