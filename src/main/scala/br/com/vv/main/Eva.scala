package br.com.vv.main

import br.com.vv.utils.BusinessTemplate
import org.apache.spark.sql.DataFrame

class Eva extends BusinessTemplate {
  override def execute(df: DataFrame): DataFrame = {
    df
  }
}