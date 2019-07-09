package br.com.vv.host

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
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
import br.com.vv.vo.StreamingConf2
import br.com.vv.vo.StreamingConf2

object StreamTemplate extends Serializable {

  def main(args: Array[String]): Unit = {

    val c = new GeneratorSchema();
    //    val h = new Hbs();

    val ss = SparkSession.builder()
      .config("spark.dynamicAllocation.enabled", true)
      .config("spark.shuffle.service.enabled", true).config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict").enableHiveSupport().getOrCreate();

    val sc = ss.sparkContext
//    val sqlContext = new SQLContext(sc)

//    val conf = new SparkConf().setAppName("Simple Streaming Application")
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

    val topics = Array("TESTE_STREAMING", "TESTE_STREAMING1", "TESTE_STREAMING2")

    val confStream = Map(
      "TESTE_STREAMING" -> new StreamingConf2("br.com.vv.main.Eva", "smartcommerce:TESTE_STREAMING"),
      "TESTE_STREAMING1" -> new StreamingConf2("br.com.vv.main.Eva", "smartcommerce:TESTE_STREAMING1"),
      "TESTE_STREAMING2" -> new StreamingConf2("br.com.vv.main.EvaCount", "smartcommerce:TESTE_STREAMING2"))

      
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

        val p = confStream.get(topic)
        val df_instance = p.get._connection.execute(df_final)
        
        df_instance.foreachPartition { forEach =>
          val tb = getTable(p.get._nm_table)
          forEach.foreach(g =>
            insertHbase(tb, g))
        }

      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

//  def executor[T <: br.com.vv.utils.BusinessTemplate](cfg: String, DfIn : DataFrame)(implicit tag: ClassTag[T]){
//    tag.runtimeClass.getConstructor(classOf[BusinessTemplate]).newInstance(cfg).asInstanceOf[T].execute(DfIn)
//  }

}