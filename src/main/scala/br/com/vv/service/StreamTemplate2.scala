package br.com.vv.service

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import br.com.vv.dao.Hbs.getTable
import br.com.vv.dao.Hbs.insertHbase
import br.com.vv.utils.GeneratorSchema
import br.com.vv.vo.StreamingConf
import br.com.vv.dao.DAO

class StreamTemplate2 extends Serializable {
  
  def executeStream(ss: SparkSession, confStream: Map[Option[String], StreamingConf]) {
    val ssc = new StreamingContext(ss.sparkContext, Seconds(10))
    val c = new GeneratorSchema();

    val kafkaParams = Map[String, Object](
      //      "bootstrap.servers" -> "hnodevp024.prd.bigdata.dc.nova:6667,hnodevp025.prd.bigdata.dc.nova:6667,hnodevp026.prd.bigdata.dc.nova:6667,hnodevp027.prd.bigdata.dc.nova:6667,hnodevp028.prd.bigdata.dc.nova:6667,hnodevp029.prd.bigdata.dc.nova:6667",
      "bootstrap.servers" -> "hnodevh002.hlg.bigdata.dc.nova:6667,hnodevh005.hlg.bigdata.dc.nova:6667",
      "group.id" -> "pedidos_stream",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "security.protocol" -> "PLAINTEXTSASL",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = confStream.map(f => f._1.get).toArray

    topics.map { topic =>
      val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](Array(topic), kafkaParams))

      val t = stream.map(m => m.value())
      
      t.foreachRDD { rdd =>
          val df_json = ss.read.option("multiLine", true).json(rdd)

          val df_final = c.normalizeDf(df_json)
          val df_instance = confStream.get(Some(topic)).get._instance.execute(df_final)

          DAO
          
//          df_instance.foreachPartition { forEach =>
//            val tb = getTable(confStream.get(Some(topic)).get._nm_table)
//            forEach.foreach(g =>
//              insertHbase(tb, g))
//          }
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }
  
  
  
}