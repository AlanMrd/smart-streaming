package br.com.vv.host

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

import br.com.vv.utils.Config

object host extends App with Serializable {
  val config = new Config()
  val streamConfig = config.getConfStreaming()

  val ss = SparkSession.builder().config("spark.dynamicAllocation.enabled", true).config("spark.shuffle.service.enabled", true).config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict").enableHiveSupport().getOrCreate();

  val ssc = new StreamingContext(ss.sparkContext, Seconds(10))

  import br.com.vv.service.StreamTemplate
  val stremExec = new StreamTemplate()
  stremExec.executeStream(ssc, ss, streamConfig)
}