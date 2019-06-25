package br.com.vv.main

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseConfiguration
import br.com.vv.dao.hdfsDAO
import br.com.vv.dao.hbaseDAO
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.rand
import br.com.vv.utils._
import br.com.vv.dao.hbaseConnection._
import org.apache.spark.sql.functions._
import br.com.vv.dao.Hbs.getTable
import br.com.vv.dao.Hbs.insertOnHbase
import br.com.vv.dao.Hbs.insertHbase

object StreamTemplate extends Serializable {

  def main(args: Array[String]): Unit = {

    val c = new GeneratorSchema();
    //    val h = new Hbs();

    val ss = SparkSession.builder()
      .config("spark.dynamicAllocation.enabled", true)
      .config("spark.shuffle.service.enabled", true).config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict").enableHiveSupport().getOrCreate();

    val sc = ss.sparkContext
    val sqlContext = new SQLContext(sc)

    val conf = new SparkConf().setAppName("Simple Streaming Application")
    val ssc = new StreamingContext(sc, Seconds(10))

    val kafkaParams = Map[String, Object](
      //      "bootstrap.servers" -> "hnodevp024.prd.bigdata.dc.nova:6667,hnodevp025.prd.bigdata.dc.nova:6667,hnodevp026.prd.bigdata.dc.nova:6667,hnodevp027.prd.bigdata.dc.nova:6667,hnodevp028.prd.bigdata.dc.nova:6667,hnodevp029.prd.bigdata.dc.nova:6667",
      "bootstrap.servers" -> "hnodevh002.hlg.bigdata.dc.nova:6667,hnodevh005.hlg.bigdata.dc.nova:6667",
      "group.id" -> "pedidos_stream",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "security.protocol" -> "PLAINTEXTSASL",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array("TESTE_STREAMING", "TESTE_STREAMING1")

    topics.map { topic =>
      val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](Array(topic), kafkaParams))

      val t = stream.map(m => m.value())

      t.foreachRDD { rdd =>
        val df_json = ss.read.option("multiLine", true).json(rdd)
        val cols = c.fieldsFinal(df_json.schema, "")
        val cols_renam = cols.map(x => x.replace(".", "_"))

        val column_names_col = cols.map(name => col(name))
        val df = df_json.select(column_names_col: _*)
        val df_new = df.toDF(cols_renam: _*)

        val df_final = c.containsArray(c.hasArray(df_new), df_new).toDF(cols_renam: _*)
        df_final.show()

        df_final.foreachPartition { forEach =>
          val tb = getTable("smartcommerce:" + topic)
//          forEach.foreach(g => insertOnHbase(tb, g))
          forEach.foreach(g => insertHbase(tb, g))
        }

      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}