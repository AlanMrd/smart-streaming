package br.com.vv.dao

import org.apache.spark.sql.DataFrame

class EventManager extends Serializable {
  var listeners: Seq[DAO] = Nil

  def addSubscribers() {
    val a = new HbaseDAO()
    val b = new MongoDAO()

    listeners = Seq(a) ++ Seq(b)
  }

  def save(df: DataFrame) {
    listeners.foreach(f => f.save(df))
  }

  def show() {
    listeners.foreach(f => print(f))
  }

}