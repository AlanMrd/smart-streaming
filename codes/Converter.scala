package br.com.vv.utils

import net.liftweb.json._
import scala.util.parsing.json._
import scala.util.matching.Regex
import net.liftweb.json._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.types.DataType

case class Compra(rowkey: Int, IdCompraEntrega: Int, IdCompra: Int, IdFreteEntregaTipo: Int)

class Converter extends Serializable {
  def jsonConverter(rddJson: String): Compra = {
    implicit val formats = DefaultFormats
    val rdd = rddJson.slice(40, 300)

    val j = parse(rdd)

    val c = (j \\ "Compra").children
    var df: Compra = null

    for (issue <- c) {
      df = new Compra((issue \ "rowkey").extract[Int], (issue \ "IdCompraEntrega").extract[Int], (issue \ "IdCompra").extract[Int], (issue \ "IdFreteEntregaTipo").extract[Int])
    }

    return df
  }

  def stringToString(rddJson: String): String = {
    val rdd = rddJson.slice(40, 600)
    return rdd
  }

  def jsonToRddRow(rddJson: String, schema: StructType, ss: SparkSession): JavaRDD[Row] ={
   return null
  }
  
  def jsonToClass(rddJson: String){
    val rdd = rddJson.slice(40, 600)  
    val json = Seq(JSON.parseFull(rdd))

  }
}

