package br.com.vv.utils

import scala.io.Source
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.ObjectMapper
import java.io.File

case class StreamConfig(nm_class: Option[String], nm_table: Option[String])

class Config {
  def getConfStreaming() {
    val file = "C:\\Users\\alan.miranda\\Documents\\conf.json"
    val json = Source.fromFile(new File(file)).getLines.mkString.trim()

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    //
    val jsonMapper = mapper.readValue(json, classOf[List[Map[String, String]]])
    val confStream = jsonMapper.map(f => f.get("topic") -> new StreamConfig(f.get("nm_class"), f.get("nm_table"))).toMap
    val topic = confStream.get(Some("TESTE_STREAMING1"))
    println(topic.get.nm_class)
    println(topic.get.nm_table)

  }
}