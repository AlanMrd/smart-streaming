package br.com.vv.dao

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

class HbaseDAO extends DAO {

  override def save(df: DataFrame) {
    df.foreachPartition { forEach =>
      val tb = HbaseDAO.getTable("")
      forEach.foreach(g => HbaseDAO.insertHbase(tb, g))
    }
  }
}

  object HbaseDAO extends Serializable {

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

    def insertHbase(hbaseTable: Table, row: Row) {
      val put = new Put(Bytes.toBytes(row.getAs("rowkey").toString()))

      row.schema.fieldNames.foreach { f =>
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