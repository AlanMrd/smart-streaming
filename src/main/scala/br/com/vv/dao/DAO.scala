package br.com.vv.dao
import org.apache.spark.sql.DataFrame

trait DAO extends Serializable {
  def save(df: DataFrame)
}