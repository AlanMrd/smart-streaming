package br.com.vv.dao

import org.apache.spark.sql.DataFrame
import br.com.vv.utils.ConfigStr

class MongoDAO(table: String, collection: String) extends DAO {
  val _table = table
  val _collection = collection
  
  override def save(df: DataFrame, confStream: ConfigStr): Unit = {
    lazy val uri ="mongodb://10.128.121.24:27017/" + _table
    val collection: String = _collection
    val method: String = "append"
    
    print(uri)
    print(collection)
    
    df.write.option("spark.mongodb.output.uri", s"${uri}.${collection}")
      .format("com.mongodb.spark.sql.DefaultSource").mode(method).save()
  }
}