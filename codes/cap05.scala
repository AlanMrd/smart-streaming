package br.com.vv.host

import br.com.vv.vo.StreamingConf
import scala.util.parsing.json.JSON
import java.io.File

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.ObjectMapper
import scala.collection.immutable
import scala.tools.asm.TypeReference
import scala.io.Source
import com.google.gson.Gson
import net.liftweb.json._

object Demo extends App {
  //  val confStream = Map(
  //    "TESTE_STREAMING" -> new StreamingConf("SumOfOrders", "smartcommerce:TEST_STREAMING"),
  //    "TESTE_STREAMING1" -> new StreamingConf("SumOfOrders", "smartcommerce:TEST_STREAMING1"),
  //    "TESTE_STREAMING2" -> new StreamingConf("SumOfOrders", "smartcommerce:TEST_STREAMING2"))
  //
  //   val p =  confStream.get("TESTE_STREAMING")
  //   if(p.isDefined){
  //     print(p.get.nm_table)
  //   }

  //  val json = """[
  //  {
  //    "topic": "TESTE_STREAMING",
  //    "nm_class": "br.com.vv.main.Eva",
  //    "nm_topic": "smartcommerce:TESTE_STREAMING"
  //  },
  //  {
  //    "topic": "TESTE_STREAMING1",
  //    "nm_class": "br.com.vv.main.Eva",
  //    "nm_topic": "smartcommerce:TESTE_STREAMING1"
  //  },
  //  {
  //    "topic": "TESTE_STREAMING2",
  //    "nm_class": "br.com.vv.main.Eva",
  //    "nm_topic": "smartcommerce:TESTE_STREAMING2"
  //  }
  //]"""

  //  case class Config(nm_class:Option[String], nm_table:Option[String])
  //  val json = """
  //  [{
  //    "topic": "TESTE_STREAMING",
  //    "nm_class": "br.com.vv.main.Eva",
  //    "nm_table": "smartcommerce:TESTE_STREAMING"
  //  },
  //  {
  //    "topic": "TESTE_STREAMING1",
  //    "nm_class": "br.com.vv.main.EvaCount",
  //    "nm_table": "smartcommerce:TESTE_STREAMING1"
  //  }]"""

  //  val file = "C:\\Users\\alan.miranda\\Documents\\conf.json"
  //  val json = Source.fromFile(new File(file)).getLines.mkString.trim()
  //
  ////  print(lines)
  //
  //  val mapper = new ObjectMapper()
  //  mapper.registerModule(DefaultScalaModule)
  ////
  //  val jsonMapper = mapper.readValue(json, classOf[List[Map[String,String]]])
  //  val confStream = jsonMapper.map(f => f.get("topic") -> new Config(f.get("nm_class"), f.get("nm_table"))).toMap
  //  val topic = confStream.get(Some("TESTE_STREAMING1"))
  //  println(topic.get.nm_class)
  //  println(topic.get.nm_table)
  //
  //  val value = jsonMapper.flatMap(f => Config(f.get("nm_topic"),f.get("nm_class"), f.get("nm_topic"))).

  val json = parse("""[
  {
    "topic": "TESTE_STREAMING",
    "nm_class": "br.com.vv.host.Eva",
    "sink": [
      {
        "name": "HBASE",
        "table": "smartcommerce:TESTE_STREAMING"
      },
      {
        "name": "MONGO",
        "table": "teste_streaming",
        "collection": "dados_teste"
      }
    ]
  },
  {
    "topic": "TESTE_STREAMING1",
    "nm_class": "br.com.vv.host.EvaCount",
    "sink": [
      {
        "name": "HBASE",
        "table": "smartcommerce:TESTE_STREAMING1"
      }
    ]
  }
]""")

  case class Sink(val name: String, val table: String, val collection: Option[String])
  case class Config(val topic: String, val nm_class: String, sink: List[Sink])

  implicit val formats = DefaultFormats
  val m = json.extract[List[Config]]

//  jsonMapper.map(f => f.get("topic") -> new StreamingConf(f.get("nm_class"), f.get("nm_table"))).toMap
//  m.flatMap(f => f.sink.map(g => g.name))
  val a = m.map(f => f.topic -> f).toMap
  
  val b = a.get("TESTE_STREAMING")
  print(b)
  
  

  //    val gson = new Gson()
  //    val jsonParsed = gson.fromJson(json, classOf[List[Map[String,String]]])
  //    print(jsonParsed.iterator()

  //  val mapper = new ObjectMapper()
  //  mapper.registerModule(DefaultScalaModule)
  //
  //  val jsonMapper = mapper.readValue(json, classOf[List[Map[String,String]]])
  //  println(jsonMapper)

  //  print(jsonMapper.flatMap(f => f.get("sink").map(g => g.)))
}