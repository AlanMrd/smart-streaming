package br.com.vv.host

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

import br.com.vv.utils.GeneratorConfig
import br.com.vv.events.EventManager
import br.com.vv.service.StreamTemplate
import br.com.vv.vo.ConfigStreaming

object SmartStreaming extends App with Serializable {
  val config = new GeneratorConfig()
  val streamConfig = config.getConfigStreaming()
   
  val ss = SparkSession.builder().config("spark.dynamicAllocation.enabled", true).config("spark.shuffle.service.enabled", true).config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict").enableHiveSupport().getOrCreate();

  val streamExec = new StreamTemplate()
  streamExec.executeStream(ss, streamConfig)
}