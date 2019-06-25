import scala.util.parsing.json._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{udf, explode}



class SchemaFree {
  def fieldsFinal(schema: StructType, prefix: String) : Seq[String] ={
  val fullName: String => String = name => if (prefix.isEmpty) name else s"$prefix.$name"
  
  schema.fields.flatMap {      
    case StructField(name, struct: StructType, _, _) => fieldsFinal(struct, fullName(name)) 
    case StructField(name, ArrayType(struct: StructType, _), _, _) => fieldsFinal(struct, fullName(name))
    case StructField(name, _, _, _)                 => Seq(fullName(name))
  }
}
 
def fieldsArray(schema: StructType, prefix: String): Seq[String] = {
  val fullName: String => String = name => if (prefix.isEmpty) name else s"$prefix.$name"

  schema.fields.flatMap {
    case StructField(name, struct: StructType, _, _) => fieldsArray(struct, fullName(name))
    case StructField(name, ArrayType(_, _), _, _) => Seq(name)
    case StructField(name, _, _, _) => Seq.empty[String] //Seq.empty[String]
  }
}

def fieldsNotArray(schema: StructType, prefix: String): Seq[String] = {
  val fullName: String => String = name => if (prefix.isEmpty) name else s"$prefix.$name"

  schema.fields.flatMap {
    case StructField(name, struct: StructType, _, _) => fieldsNotArray(struct, fullName(name))
    case StructField(name, ArrayType(_, _), _, _) => Seq.empty[String]
    case StructField(name, _, _, _) => Seq(name)
  }
}

def udfCol(df: DataFrame, fields: Seq[String]): Seq[DataFrame] ={
  fields.map(f => df.withColumn(f+"exploded" , explode(df(f))).drop(f)).toSeq
}

def udfId(df: Seq[DataFrame]): Seq[DataFrame] ={
  df.map(f => f.withColumn("index", monotonically_increasing_id)).toSeq
}

def hasArray(df: DataFrame): Seq[String] = {
  df.schema.fields.flatMap {
    case StructField(name, ArrayType(_, _), _, _) => Seq("Array")
    case _ => Seq.empty[String]
  }
}

def AA(df_new: DataFrame): DataFrame = {
    val array_cols = fieldsArray(df_new.schema, "")
    val array_not_cols = fieldsNotArray(df_new.schema, "")

    val df_cols = udfCol(df_new, array_cols)

    val col_array_not_cols = array_not_cols.map(name => col(name))
    val dfnotarraycol = df_cols.head.select(col_array_not_cols: _*).withColumn("index", monotonically_increasing_id)

    val col_array_cols = array_cols.map(name => col(name + "exploded")) :+ col("index")

    val df_index = udfId(df_cols)
    val df_reduce = df_index.reduce(_.join(_, Seq("index"))).select(col_array_cols: _*)

    dfnotarraycol.join(df_reduce, dfnotarraycol("index") === df_reduce("index"), "outer").drop("index")

  }

def containsArray(hasArr: Seq[String], df: DataFrame): DataFrame = {
  hasArr.contains("Array") match{
    case true => AA(df)
    case false => df
  }
}


//val df = spark.read.option("multiLine", true).json("/tmp/menu.json")
//
//val cols = fieldsFinal(df.schema, "")
//val column_names_col = cols.map(name => col(name))
//val cols_renam = cols.map(x => x.replace(".","_"))
//
//val df_new = df.select(column_names_col:_*).toDF(cols_renam)
//
//containsArray(hasArray(df_new), df_new)
}
//
//
