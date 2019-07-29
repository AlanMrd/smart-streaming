package br.com.vv.vo

class Sink(val name: String, val table: String, val collection: Option[String]) {
  val _name: String = name
  val _table: String = table
  val _collection: Option[String] = collection
}