package br.com.vv.main
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession

case class Foto(id: String, owner: String, server: Int, title: String, tags: Option[List[String]])

object TestKafkaSparkStreaming {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf() //.setMaster("spark://192.168.177.120:7077")
      .setAppName("SparkStreamKaflaWordCount Demo")
      .set("spark.streaming.concurrentJobs", "8")
    val ss = SparkSession
      .builder()
      .config(conf)
      .appName(args.mkString(" "))
      .getOrCreate()

    val topicsArr: Array[String] = Array(
      "betadbserver1.copytrading.t_trades",
      "betadbserver1.copytrading.t_users",
      "betadbserver1.account.s_follower",
      "betadbserver1.copytrading.t_followorder",
      "betadbserver1.copytrading.t_follow",
      "betadbserver1.copytrading.t_activefollow",
      "betadbserver1.account.users",
      "betadbserver1.account.user_accounts")

    var group = "con-consumer-group111" + (new util.Random).nextInt(10000)
    val kafkaParam = Map(
      "bootstrap.servers" -> "beta-hbase02:9092,beta-hbase03:9092,beta-hbase04:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "earliest", //"latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))
    val ssc = new StreamingContext(ss.sparkContext, Seconds(4))
    //val streams =
    topicsArr.foreach { //.slice(0,1)
      topic =>
        val newTopicsArr = Array(topic)
        val stream =
          KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](newTopicsArr, kafkaParam))
        stream.map(s => (s.key(), s.value())).print();
    }
    /*
    val unifiedStream = ssc.union(streams)
    unifiedStream.repartition(2)
    unifiedStream.map(s =>(s.key(),s.value())).print()
    */
    /*
    unifiedStream.foreachRDD{ rdd =>
      rdd.foreachPartition{ partitionOfRecords =>
        partitionOfRecords.foreach{ record =>

        }
      }
    }
    */
    ssc.start();
    ssc.awaitTermination();
  }

}
  