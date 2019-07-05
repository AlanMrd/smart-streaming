package br.com.vv.main

import br.com.vv.utils.BusinessTemplate
import org.apache.spark.sql.DataFrame

class EvaCount extends BusinessTemplate {
  override def execute(df: DataFrame): DataFrame = {
    print(df.count())
    df
  }  
}