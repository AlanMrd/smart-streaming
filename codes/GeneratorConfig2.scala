package br.com.vv.utils

import java.io.File
import java.net.URI

import scala.io.Source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import br.com.vv.events.EventManager
import br.com.vv.vo.StreamingConf
import net.liftweb.json._
import br.com.vv.vo.Sink

case class StreamConfig(nm_class: Option[String], nm_table: Option[String])
//case class Sink(val name: String, val table: String, val collection: Option[String])
//case class ConfigStr(val topic: String, val nm_class: String, sink: List[Sink])

//class Sink(val name: String, val table: String, val collection: Option[String]) {
//  val _name: String = name
//  val _table: String = table
//  val _collection: Option[String] = collection
//}

class ConfigStr(val topic: String, val nm_class: String, sink: List[Sink]) {
  val _topic: String = topic
  val _nm_class: String = nm_class
  val _sink: List[Sink] = sink
  val _instance: BusinessTemplate = StreamingConfig.getInstance
  val _event = new EventManager(_sink)

  object StreamingConfig extends Serializable {
    def getInstance(): BusinessTemplate = {
      val cl = Class.forName(_nm_class).asInstanceOf[Class[_ <: br.com.vv.utils.BusinessTemplate]]
      lazy val instance = cl.newInstance()
//      println(instance)
      instance
    }
  }
}

class GetConfig extends Serializable {
  def getConfStreaming(): Map[Option[String], StreamingConf] = {
    val hdfs = FileSystem.get(new URI("hdfs://vvdatahlgnnha"), new Configuration())
    val path = new Path("/tmp/conf.json")
    val stream = hdfs.open(path)
    val json = Source.fromInputStream(stream).getLines().mkString.trim()
    //    val json = Source.fromFile(new File(file)).getLines.mkString.trim()

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val jsonMapper = mapper.readValue(json, classOf[List[Map[String, String]]])
    jsonMapper.map(f => f.get("topic") -> new StreamingConf(f.get("nm_class"), f.get("nm_table"))).toMap
  }

  def getConfig(): Map[Option[String], StreamingConf] = {
    val file = "C:\\Users\\alan.miranda\\Documents\\conf.json"
    //    val file = "/pgm/batch/conf.json"
    val json = Source.fromFile(new File(file)).getLines.mkString.trim()

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    //
    val jsonMapper = mapper.readValue(json, classOf[List[Map[String, String]]])
    jsonMapper.map(f => f.get("topic") -> new StreamingConf(f.get("nm_class"), f.get("nm_table"))).toMap

  }

  def getConfigStreaming(): Map[String, ConfigStr] = {
    val hdfs = FileSystem.get(new URI("hdfs://vvdatahlgnnha"), new Configuration())
    val path = new Path("/tmp/config.json")
    val stream = hdfs.open(path)
    val json = parse(Source.fromInputStream(stream).getLines().mkString.trim())
    
    implicit val formats = DefaultFormats
    val classExtract = json.extract[List[ConfigStr]]

    classExtract.map(f => f.topic -> f).toMap
  }
}