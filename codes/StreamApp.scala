package br.com.vv.host

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
import net.liftweb.json._
import br.com.vv.utils._
import br.com.vv.dao.hbaseConnection._

object StreamApp extends Serializable {

  def main(args: Array[String]): Unit = {

    val c = new Converter();
    val h = new hbaseDAO();
    
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

    val topics = Array("TESTE_STREAMING")
    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    val t = stream.map(m => c.jsonConverter(m.value()))

    t.foreachRDD { rdd =>
      import sqlContext.implicits._
      val df = rdd.toDF()

      rdd.foreachPartition { fpartition =>
        val tb = table
        
        fpartition.foreach { f => 
          
          val put = new Put(Bytes.toBytes(f.rowkey.toString()))

          put.addColumn(Bytes.toBytes("dados"), Bytes.toBytes("idCompra"), Bytes.toBytes(f.IdCompra.toString()))
          put.addColumn(Bytes.toBytes("dados"), Bytes.toBytes("IdCompraEntrega"), Bytes.toBytes(f.IdCompraEntrega.toString()))
          put.addColumn(Bytes.toBytes("dados"), Bytes.toBytes("IdFreteEntregaTipo"), Bytes.toBytes(f.IdFreteEntregaTipo.toString()))

          tb.put(put)
        }
        
        table.close()
      }
    }

    ssc.start()
    ssc.awaitTermination()
    connection.close()
  }
}