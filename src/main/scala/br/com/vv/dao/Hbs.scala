package br.com.vv.dao

import scala.util.{ Failure, Success, Try }

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ Connection, ConnectionFactory, Get, Put }
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory
import org.apache.hadoop.hbase.client.Table
import org.apache.spark.sql.Row
import scala.util.Random

import org.apache.hadoop.hbase.HBaseConfiguration

object Hbs extends Serializable {

  @transient lazy val hBaseConfiguration = HBaseConfiguration.create()
  @transient lazy val conn = ConnectionFactory.createConnection(hBaseConfiguration)

  val log = LoggerFactory.getLogger(getClass)

  def insertOrUpdateTableHbase(hbaseTable: String, put: Put) {
    @transient lazy val table = conn.getTable(TableName.valueOf(Bytes.toBytes(hbaseTable)))
    table.put(put)
    table.close()
  }

  def getTable(hbaseTable: String): Table = {
    @transient lazy val table = conn.getTable(TableName.valueOf(Bytes.toBytes(hbaseTable)))
    table
  }

  def insertOnHbase(hbaseTable: Table, row: Row) {
    val r = Random

    //    row.schema.fieldNames
    val put = new Put(Bytes.toBytes(r.nextInt().toString()))

    put.addColumn(Bytes.toBytes("dados"), Bytes.toBytes("idcompra"), Bytes.toBytes(row.getAs("compra_idcompra").toString()))
    put.addColumn(Bytes.toBytes("dados"), Bytes.toBytes("idfreteentregatipo"), Bytes.toBytes(row.getAs("compra_idfreteentregatipo").toString()))
    put.addColumn(Bytes.toBytes("dados"), Bytes.toBytes("idcompraentrega"), Bytes.toBytes(row.getAs("compra_idcompraentrega").toString()))

    hbaseTable.put(put)
  }

  def insertHbase(hbaseTable: Table, row: Row) {
    val r = Random
    val put = new Put(Bytes.toBytes(r.nextInt().toString()))
    
    row.schema.fieldNames.foreach{f =>
      put.addColumn(Bytes.toBytes("dados"), Bytes.toBytes(f), Bytes.toBytes(row.getAs(f).toString()))
      }
    
    hbaseTable.put(put)
  }

  def getRowFamilyHbase(hbaseTable: String, rowKey: String, rowFamily: String) = {
    val get = new Get(Bytes.toBytes(rowKey)).addFamily(Bytes.toBytes(rowFamily))
    @transient lazy val table = conn.getTable(TableName.valueOf(Bytes.toBytes(hbaseTable)))
    val ret = table.get(get)
    table.close()
    ret
  }

  def close(): Unit = {
    conn.close()
  }
}

//trait HBaseDAOComponent {
//  lazy val hBaseDAO = new HBaseDAO
//}
//}