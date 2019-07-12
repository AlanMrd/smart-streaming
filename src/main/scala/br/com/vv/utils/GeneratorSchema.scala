package br.com.vv.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

class GeneratorSchema extends Serializable {
  def fieldsFinal(schema: StructType, prefix: String): Seq[String] = {
    val fullName: String => String = name => if (prefix.isEmpty) name else s"$prefix.$name"

    schema.fields.flatMap {
      case StructField(name, struct: StructType, _, _)               => fieldsFinal(struct, fullName(name))
      case StructField(name, ArrayType(struct: StructType, _), _, _) => fieldsFinal(struct, fullName(name))
      case StructField(name, _, _, _)                                => Seq(fullName(name))
    }
  }

  def fieldsArray(schema: StructType, prefix: String): Seq[String] = {
    val fullName: String => String = name => if (prefix.isEmpty) name else s"$prefix.$name"

    schema.fields.flatMap {
      case StructField(name, struct: StructType, _, _) => fieldsArray(struct, fullName(name))
      case StructField(name, ArrayType(_, _), _, _)    => Seq(name)
      case StructField(name, _, _, _)                  => Seq.empty[String] //Seq.empty[String]
    }
  }

  def fieldsNotArray(schema: StructType, prefix: String): Seq[String] = {
    val fullName: String => String = name => if (prefix.isEmpty) name else s"$prefix.$name"

    schema.fields.flatMap {
      case StructField(name, struct: StructType, _, _) => fieldsNotArray(struct, fullName(name))
      case StructField(name, ArrayType(_, _), _, _)    => Seq.empty[String]
      case StructField(name, _, _, _)                  => Seq(name)
    }
  }

  def dfExplodeCols(df: DataFrame, fields: Seq[String]): Seq[DataFrame] = {
    fields.map(f => df.withColumn(f + "exploded", explode(df(f))).drop(f)).toSeq
  }

  def putIndexOnDf(df: Seq[DataFrame]): Seq[DataFrame] = {
    df.map(f => f.withColumn("index", monotonically_increasing_id)).toSeq
  }

  def hasArray(df: DataFrame): Seq[String] = {
    df.schema.fields.flatMap {
      case StructField(name, ArrayType(_, _), _, _) => Seq("Array")
      case _                                        => Seq.empty[String]
    }
  }

  def renameColumn(arr: Array[String]): List[String] = {
    val c = arr.filterNot(x => x.endsWith("exploded")).toList
    val b = arr.filter(x => x.endsWith("exploded")).map(x => x.dropRight(8)).toList
    val d = c ++ b
    d
  }

  def dfwithArray(df_new: DataFrame): DataFrame = {
    val array_cols = fieldsArray(df_new.schema, "")
    val array_not_cols = fieldsNotArray(df_new.schema, "")

    val df_cols = dfExplodeCols(df_new, array_cols)

    val col_array_not_cols = array_not_cols.map(name => col(name))
    val dfnotarraycol = df_cols.head.select(col_array_not_cols: _*).withColumn("index", monotonically_increasing_id)

    val col_array_cols = array_cols.map(name => col(name + "exploded")) :+ col("index")

    val df_index = putIndexOnDf(df_cols)
    val df_reduce = df_index.reduce(_.join(_, Seq("index"))).select(col_array_cols: _*)

    val df_col = dfnotarraycol.join(df_reduce, dfnotarraycol("index") === df_reduce("index"), "outer").drop("index")
    val renam_columns = renameColumn(df_col.columns)

    df_col.toDF(renam_columns: _*).select(df_new.columns.map(x => col(x)): _*)
  }

  def normalizeDf(dfarrayNew: DataFrame): DataFrame = {
    val cols: Seq[String] = fieldsFinal(dfarrayNew.schema, "")
    val cols_renam = cols.map(x => x.replace(".", "_"))

    val column_names_col = cols.map(name => col(name))

    val df: DataFrame = dfarrayNew.select(column_names_col: _*)
    val df_new = df.toDF(cols_renam: _*)

    containsArray(hasArray(df_new), df_new).toDF(cols_renam: _*)
  }

  def containsArray(hasArr: Seq[String], df: DataFrame): DataFrame = {
    hasArr.contains("Array") match {
      case true  => dfwithArray(df)
      case false => df
    }
  }
}