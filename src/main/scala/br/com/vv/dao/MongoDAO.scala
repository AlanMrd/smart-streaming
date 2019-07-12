package br.com.vv.dao

import org.apache.spark.sql.DataFrame

class MongoDAO extends DAO {
  override def save(df: DataFrame): Unit = {
    lazy val uri ="mongodb://10.128.121.24:27017/testestreaming"
    val collection: String = "dados_teste"
    val method: String = ""
    
    df.write
      .option("spark.mongodb.output.uri", s"${uri}.${collection}")
      .format("com.mongodb.spark.sql.DefaultSource").mode(method).save()
  }
}