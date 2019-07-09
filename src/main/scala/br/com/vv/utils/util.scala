package br.com.vv.utils

import scala.io.Source
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.ObjectMapper
import java.io.File
import br.com.vv.vo
import br.com.vv.vo.StreamingConf
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import scala.io.Source

case class StreamConfig(nm_class: Option[String], nm_table: Option[String])

class Config extends Serializable {
  def getConfStreaming(): Map[Option[String], StreamingConf] = {
    //    val file = "C:\\Users\\alan.miranda\\Documents\\conf.json"
    //    val file = "/pgm/batch/conf.json"

    val hdfs = FileSystem.get(new URI("hdfs://vvdatahlgnnha"), new Configuration())
    val path = new Path("/tmp/conf.json")
    val stream = hdfs.open(path)
    val json = Source.fromInputStream(stream).getLines().mkString.trim()
    //    val json = Source.fromFile(new File(file)).getLines.mkString.trim()

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    //
    val jsonMapper = mapper.readValue(json, classOf[List[Map[String, String]]])
    jsonMapper.map(f => f.get("topic") -> new StreamingConf(f.get("nm_class"), f.get("nm_table"))).toMap
  }

  def getConfig() : Map[Option[String], StreamingConf] = {
    val file = "C:\\Users\\alan.miranda\\Documents\\conf.json"
    //    val file = "/pgm/batch/conf.json"
    val json = Source.fromFile(new File(file)).getLines.mkString.trim()

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    //
    val jsonMapper = mapper.readValue(json, classOf[List[Map[String, String]]])
    jsonMapper.map(f => f.get("topic") -> new StreamingConf(f.get("nm_class"), f.get("nm_table"))).toMap

  }

}