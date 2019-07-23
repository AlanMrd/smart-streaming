//package br.com.vv.events
//
//import org.apache.spark.sql.DataFrame
//import br.com.vv.dao.DAO
//import br.com.vv.dao.HbaseDAO
//import br.com.vv.dao.MongoDAO
//import scala.collection.Seq
//import br.com.vv.utils.ConfigStr
//import br.com.vv.utils.Sink
//
//class EventManager1(sink: List[Sink]) extends Serializable {
//  var listeners: Seq[DAO] = addSubscr(sink)
//
////  def save(df: DataFrame, confStream: Map[String, ConfigStr]) {
////    listeners.foreach(f => f.save(df, confStream))
////  }
//
//  def showSubscribers() {
//    listeners.map(f => print(f))
//  }
//
//  def addSubscr(sink: List[Sink]): Seq[DAO] = {
//    var listen: Seq[DAO] = Nil
//    listen ++ sink.map(f => checkDependey(f.name))
//  }
//
//  def checkDependey(dep: String): DAO = {
//    val a = dep match {
//      case (e: String) if (e == "HBASE") => new HbaseDAO()
//      case _                             => new MongoDAO()
//    }
//    a
//  }
//}