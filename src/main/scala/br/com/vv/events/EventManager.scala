package br.com.vv.events

import org.apache.spark.sql.DataFrame
import br.com.vv.dao.DAO
import br.com.vv.dao.HbaseDAO
import br.com.vv.dao.MongoDAO
import scala.collection.Seq
import br.com.vv.utils.ConfigStr
import br.com.vv.utils.Sink

class EventManager(sink: List[Sink]) extends Serializable {
  var listeners: Seq[DAO] = addSubscr(sink)

  def save(df: DataFrame, confStream: ConfigStr) {
    listeners.foreach(f => f.save(df, confStream))
  }

  def showSubscribers() {
    listeners.map(f => print(f))
  }

  def addSubscr(sink: List[Sink]): Seq[DAO] = {
//    var listen: Seq[DAO] = Nil

    sink.flatMap { f =>
      f.name match {
        case (e: String) if (e == "HBASE") => Seq(new HbaseDAO(f.table))
        case _                             => Seq(new MongoDAO(f.table, getWrapParameter(f.collection)))
      }
    }
  }

  def getWrapParameter(parameter: Option[String]): String = {
    parameter match {
      case Some(e: String) => e
      case None            => throw new Exception("Classe ou tabela não existe!!!")
    }
  }

  //  def addSubscr(sink: List[Sink]): Seq[DAO] = {
  //    var listen: Seq[DAO] = Nil
  //
  //    listen ++ sink.map { f =>
  //      f.name match {
  //        case (e: String) if (e == "HBASE") => new HbaseDAO(f.table)
  //        case _                             => new MongoDAO(f.table, getWrapParameter(f.collection))
  //      }
  //    }
  //  }

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

  //    val a = dep match {
  //      case (e: String) if (e == "HBASE") => new HbaseDAO()
  //      case _                             => new MongoDAO()
  //    }
  //    a

}