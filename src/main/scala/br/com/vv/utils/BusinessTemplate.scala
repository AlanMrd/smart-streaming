package br.com.vv.utils
import org.apache.spark.sql.DataFrame

trait BusinessTemplate extends Serializable {
  def execute(DfIn: DataFrame): DataFrame
}