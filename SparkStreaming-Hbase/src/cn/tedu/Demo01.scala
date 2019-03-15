package cn.tedu

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

object Demo01 {
  def main(args: Array[String]): Unit = {
    //1.创建sc
    val conf = new SparkConf();
    conf.setAppName("Spark_Hbase")
    conf.setMaster("local");
    val sc = new SparkContext(conf);

    //2.创建hbase配置对象,指定基本配置
    val hbaseConf = HBaseConfiguration.create();
    hbaseConf.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

    //3.创建JobConf,指定输出的表名
    val jobConf = new JobConf(hbaseConf);
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "tab1")

    //4.准备数据
    val rdd = sc.makeRDD(Array("001,zhang,19", "002,li,21", "003,wang,33"));
    val toHBaseRDD = rdd.map(_.split(",")).map(arr => {
      val put = new Put(arr(0).getBytes);
      put.add("cf1".getBytes, "name".getBytes, arr(1).getBytes)
      put.add("cf1".getBytes, "age".getBytes, arr(2).getBytes)
      (new ImmutableBytesWritable, put)
    })
    //5.向HBases写入数据
    toHBaseRDD.saveAsHadoopDataset(jobConf);
  }
}