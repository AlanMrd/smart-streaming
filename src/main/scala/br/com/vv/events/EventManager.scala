package br.com.vv.events

import scala.collection.Seq

import org.apache.spark.sql.DataFrame
import br.com.vv.dao.DAO
import br.com.vv.dao.HbaseDAO
import br.com.vv.dao.MongoDAO
import br.com.vv.vo.ConfigStreaming
import br.com.vv.vo.Sink
import br.com.vv.utils.GeneralUtil.getWrapParameter

class EventManager(sink: List[Sink]) extends Serializable {
  val listeners: Seq[DAO] = addSubscr(sink)

  def save(df: DataFrame) {
    listeners.foreach(f => f.save(df))
  }

  def addSubscr(sink: List[Sink]): Seq[DAO] = {
    sink.flatMap { f =>
      f.name match {
        case (e: String) if (e == "HBASE") => Seq(new HbaseDAO(f.table))
        case _                             => Seq(new MongoDAO(f.table, getWrapParameter(f.collection)))
      }
    }
  }
}