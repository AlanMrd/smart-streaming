package br.com.vv.dao

import org.apache.spark.sql.DataFrame
import br.com.vv.vo.ConfigStreaming

trait DAO extends Serializable {
  def save(df: DataFrame)
}