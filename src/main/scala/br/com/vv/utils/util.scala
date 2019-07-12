package br.com.vv.utils

import java.io.File
import java.net.URI

import scala.io.Source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import br.com.vv.vo.StreamingConf

case class StreamConfig(nm_class: Option[String], nm_table: Option[String])

class Config extends Serializable {
  def getConfStreaming(): Map[Option[String], StreamingConf] = {
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