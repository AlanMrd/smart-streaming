package br.com.vv.host

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

import br.com.vv.utils.Config

object SmartStreaming extends App with Serializable {
  val config = new Config()
  val streamConfig = config.getConfStreaming()

  val ss = SparkSession.builder().config("spark.dynamicAllocation.enabled", true).config("spark.shuffle.service.enabled", true).config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict").enableHiveSupport().getOrCreate();

  import br.com.vv.service.StreamTemplate2
  val stremExec = new StreamTemplate2()
  stremExec.executeStream(ss, streamConfig)
}