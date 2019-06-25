package br.com.vv.dao

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Table
import scala.util.Random
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.{ functions, Column, DataFrame, SQLContext }
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

object hbaseConnection extends Serializable {
  lazy val connection = {
    val config = HBaseConfiguration.create();
    val connection = ConnectionFactory.createConnection(config);
    connection
  }

  lazy val table = {
    val table = connection.getTable(TableName.valueOf("smartcommerce:teste"))
    table
  }

  def getTable(tb: String): Table = {
    lazy val table = connection.getTable(TableName.valueOf(tb))
    table
  }

}

class hbaseDAO extends Serializable {
  def showHbase(row: Row){
    print(row)
  }
  
  def putOnHbase(row: Row) {
    val r = Random

    val put = new Put(Bytes.toBytes(r.nextInt().toString()))

    put.addColumn(Bytes.toBytes("dados"), Bytes.toBytes("idcompra"), Bytes.toBytes(row.getAs("compra_idcompra").toString()))
    put.addColumn(Bytes.toBytes("dados"), Bytes.toBytes("idfreteentregatipo"), Bytes.toBytes(row.getAs("compra_idfreteentregatipo").toString()))
    put.addColumn(Bytes.toBytes("dados"), Bytes.toBytes("idcompraentrega"), Bytes.toBytes(row.getAs("compra_idcompraentrega").toString()))

    hbaseConnection.table.put(put)

  }
}

