package br.com.vv.utils

import java.net.URI
import scala.io.Source
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import net.liftweb.json._
import br.com.vv.vo.ConfigStreaming

class GeneratorConfig extends Serializable {

  def getConfigStreaming(): Map[String, ConfigStreaming] = {
    val hdfs = FileSystem.get(new URI("hdfs://vvdatahlgnnha"), new Configuration())
    val path = new Path("/tmp/config.json")
    val stream = hdfs.open(path)
    val json = parse(Source.fromInputStream(stream).getLines().mkString.trim())

    implicit val formats = DefaultFormats
    val classExtract = json.extract[List[ConfigStreaming]]

    classExtract.map(f => f._topic -> f).toMap
  }
}