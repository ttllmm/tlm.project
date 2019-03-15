package cn.tedu

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf

object Demo01 {
  def main(args: Array[String]): Unit = {
    //1.创建sc
    val conf = new SparkConf();
    conf.setAppName("flux_spark")
    conf.setMaster("local[5]")
    val sc = new SparkContext(conf)
    
    //2.将sc包装成ssc
    val ssc = new StreamingContext(sc,Seconds(3))
   
    //3.通过KafkaUtils消费kafka中指定主题的数据
    val stream = KafkaUtils.createStream(ssc, "10.8.24.109:2181", "gx1", Map("fluxtopic"->1));
    
    //4.从流中得到数据直接打印
    stream.map(_._2).print()
    
    //5.启动流
    ssc.start();
    ssc.awaitTermination()
  }
}