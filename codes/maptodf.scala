package br.com.vv.host

import scala.util.parsing.json._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import scala.collection.TraversableLike
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import scala.collection.mutable.ArraySeq

object stream extends App {
//  val col = List(Seq("a", "b", "c"), Seq("a", "b", "c"))
//  val b = col.flatMap(f => f)
//  println(col)
//  println(b)
  
  val arr = Array("quiz_maths_q1_answer","quiz_maths_q1_optionsexploded","quiz_maths_q1_question","quiz_maths_q2_answer","quiz_maths_q2_optionsexploded","quiz_maths_q2_question","quiz_sport_q1_answer","quiz_sport_q1_optionsexploded","quiz_sport_q1_question")
  val c = arr.filterNot(x=> x.endsWith("exploded")).toList
  val b = arr.filter(x=> x.endsWith("exploded")).map(x => x.dropRight(8)).toList
  val d = c ++ b
  print(d)
//  arr.map(x => println(x.endsWith("exploded")))
}

class GetColumnsDf {

def renameColumn(arr: Array[String]): List[String] = {
  val c = arr.filterNot(x=> x.endsWith("exploded")).toList
  val b = arr.filter(x=> x.endsWith("exploded")).map(x => x.dropRight(8)).toList
  val d = c ++ b
  d
}
  
  def children(colname: String, df: DataFrame) = {
    val parent = df.schema.fields.filter(_.name == colname).headOption
    val fields = parent.get.dataType match {
      case x: StructType => x.fields
      case _             => Array.empty[StructField]
    }
    fields.map(x => col(s"$colname.${x.name}"))

  }

  //  def fieldsAll(df: DataFrame, c: String) = df.schema(c) match {
  //    case StructField(_, ArrayType(ArrayType(ss: StructType, _), _), _, _) =>
  //      ss.fields map { s =>
  //        (s.name, s.dataType)
  //      }
  //  }
  
  def normalizeDF2(ds: DataFrame) {
       ds.schema.fields.map{f => f.name}
     
   }
  
  def normal(): Array[String] = {
    var c = Array[String]()
    
    val b = Array("quiz_maths_q1_answer","quiz_maths_q1_optionsexploded","quiz_maths_q1_question","quiz_maths_q2_answer","quiz_maths_q2_optionsexploded","quiz_maths_q2_question","quiz_sport_q1_answer","quiz_sport_q1_optionsexploded","quiz_sport_q1_question")
     for(a <- b){
        if (a.endsWith("exploded"))
          c +: a.dropRight(8)
        else
          c +: a
    }
    
    return c
  }
    
  

  
  def fieldsNotArray(schema: StructType, prefix: String): Seq[String] = {
    val fullName: String => String = name => if (prefix.isEmpty) name else s"$prefix.$name"

    schema.fields.flatMap {
      case StructField(name, struct: StructType, _, _) => fieldsNotArray(struct, fullName(name))
      case StructField(name, ArrayType(_, _), _, _) => Seq.empty[String]
      case StructField(name, _, _, _) => Seq(name)
      case _ => Seq[String]("index")
    }
  }
  
  
  def generatorSchemaFull(schema: StructType): StructType ={
    StructType(schema.map(f => f.dataType match {
          case s: StructType => f.copy(nullable = true, dataType = generatorSchemaFull(s))
          case i: IntegerType => f.copy(dataType = StringType)
          case i: ShortType => f.copy(dataType = IntegerType)
          case d: TimestampType => f.copy(dataType = DateType)
          case o => f.copy(dataType = o)
    })
    )
  }
  
//  def normalizeDF3(ds: DataFrame): DataFrame = {
//     
//      ds.schema.fields.map{f => f.dataType match { 
//         case i: ShortType => val df = ds.withColumn(f.name, col(f.name).cast("int"))
//         case t: TimestampType => 
//         case _ => 
//         }     
//       }
//     }
  
  
//    case i: IntegerType => f.copy(dataType = StringType)
//          case i: ShortType => f.copy(dataType = IntegerType)
//          case d: TimestampType => f.copy(dataType = DateType)
//          case o => f.copy(dataType = o)
  
  def updateFieldsToNullable(structType: StructType): StructType = {
    StructType(structType.map(f => f.dataType match {
      case d: ArrayType =>
        val element = d.elementType match {
          case s: StructType => updateFieldsToNullable(s)
          case _ => d.elementType}
        f.copy(nullable = true, dataType = ArrayType(element, d.containsNull))
      case s: StructType => f.copy(nullable = true, dataType = updateFieldsToNullable(s))
      case _ => f.copy(nullable = true)
    })
    )
  }
  
  def generatorSchema(schema: StructType): Seq[String] = {
    schema.fields.flatMap {  
      case StructField(name, ShortType, _, _)                                => Seq("Short")
      case StructField(name, StringType, _, _)                                => Seq("String")
      case _                              =>                                      Seq("Nenhum")
      
    }
  }

  def AA(df_new: DataFrame): DataFrame = {
    val r_df = df_new.toDF("a", "b", "c", "d")

    val array_cols = fieldsArray(r_df.schema, "")
    val array_not_cols = fieldsNotArray(r_df.schema, "")

    val df_cols = udfCol(r_df, array_cols)

    val col_array_not_cols = array_not_cols.map(name => col(name))
    val dfnotarraycol = df_cols.head.select(col_array_not_cols: _*).withColumn("index", monotonically_increasing_id)

    val col_array_cols = array_cols.map(name => col(name + "exploded")) :+ col("index")

    val df_index = udfId(df_cols)
    val df_reduce = df_index.reduce(_.join(_, Seq("index"))).select(col_array_cols: _*)

    dfnotarraycol.join(df_reduce, dfnotarraycol("index") === df_reduce("index"), "outer").drop("index")

  }
  
//  def normalizeDF(ds: DataFrame): DataFrame = {
//   val cols = for (e <- ds.columns) yield col(e)
//   ds.columns.flatMap( f => {
//     val new_column = s"${f}_new"
//     df.schema(f).dataType match {
//       case ShortType => ds.withColumn(new_column, col(f).cast("int")).drop(f).withColumnRenamed(new_column, f).select(cols: _*)
//       case TimestampType => ds.withColumn(new_column, col(coluna).cast("date")).drop(coluna).withColumnRenamed(new_column, coluna).select(cols: _*)
//       case _ => df
//     }
//   })
//  }
//  
//  def normalizeDF(ds: DataFrame): DataFrame = {
//   val cols = for (e <- ds.columns) yield col(e)
//   var df = ds
//   for(coluna <- ds.columns) {
//     val new_column = s"${coluna}_new"
//     df = df.schema(coluna).dataType match {
//       case ShortType => df.withColumn(new_column, col(coluna).cast("int")).drop(coluna).withColumnRenamed(new_column, coluna).select(cols: _*)
//       case TimestampType => df.withColumn(new_column, col(coluna).cast("date")).drop(coluna).withColumnRenamed(new_column, coluna).select(cols: _*)
//       case _ => df
//     }
//   }
  
  
//  def isNested(df: DataFrame): {
//    df.schema.fields.flatMap { field => field.dataType match {
//          case ArrayType(_, _) => "Tem"
//          case _               => "Não tem"
//        }
//      }
//  }
  
   
   
   
  def hasArray(df: DataFrame): Seq[String] = {
    df.schema.fields.flatMap {
      case StructField(name, ArrayType(_, _), _, _) => Seq("Array")
      case _ => Seq.empty[String]
    }
  }
  
    def containsArray(hasArr: Seq[String], df: DataFrame): DataFrame = {
      hasArr.contains("Array") match{
        case true => AA(df)
        case false => df
      }
  }

  def helper(schema: StructType, prefix: String): Seq[String] = {
    val fullName: String => String = name => if (prefix.isEmpty) name else s"$prefix.$name"

    schema.fields.flatMap {
      case StructField(name, inner: StructType, _, _) => fullName(name) +: helper(inner, fullName(name))
      case StructField(name, _, _, _)                 => Seq(fullName(name))
    }
  }

  def fieldsNotArrayA(schema: StructType, prefix: String): Seq[String] = {
    val fullName: String => String = name => if (prefix.isEmpty) name else s"$prefix.$name"

    schema.fields.flatMap {
      case StructField(name, struct: StructType, _, _) => fieldsNotArray(struct, fullName(name))
      case StructField(name, ArrayType(_, _), _, _)    => Seq.empty[String]
      case StructField(name, _, _, _)                  => Seq(name)
    }
  }

  def helperLast(schema: StructType, prefix: String): Seq[String] = {
    val fullName: String => String = name => if (prefix.isEmpty) name else s"$prefix.$name"

    schema.fields.flatMap {
      case StructField(name, _, _, _) => Seq(fullName(name))
    }
  }

  //  def fields(df: DataFrame, c: String) = df.schema(c) match {
  //    case StructField(_, ArrayType(ArrayType(ss: StructType, _), _), _, _) =>
  //      ss.fields map { s =>
  //        (s.name, s.dataType)
  //      }
  //  }

  def helperon(schema: StructType, prefix: String): Seq[String] = {
    val fullName: String => String = name => if (prefix.isEmpty) name else s"$prefix.$name"

    schema.fields.flatMap {
      case StructField(name, inner: StructType, _, _) => helperon(inner, fullName(name))
      case StructField(name, _, _, _)                 => Seq(fullName(name))
    }
  }

  def helpe(schema: StructType, prefix: String): Seq[String] = {
    val fullName: String => String = name => if (prefix.isEmpty) name else s"$prefix.$name"

    schema.fields.flatMap {
      case StructField(name, inner: StructType, _, _) => helpe(inner, fullName(name))
      case StructField(name, _, _, _)                 => Seq(fullName(name))
    }
  }

  def he(schema: StructType, prefix: String) {
    schema.fields.flatMap {
      case StructField(_, ArrayType(name, _), _, _) => Seq(name)
    }
  }

  def he(ArrayColum: Seq[String]) {
    for (arr <- ArrayColum)
      print(arr)
  }

  def getArrayCol(arrayCol: Seq[String]) {
    val emptySeq = Seq.empty[String]
    emptySeq.zip(arrayCol.map(x => x.toSeq))
  }

  def extractArrayCols(schema: StructType, prefix: String): Seq[String] = {
    schema.fields.flatMap {
      case StructField(name, struct: StructType, _, _) => extractArrayCols(struct, prefix + name + ".")
      case StructField(name, ArrayType(_, _), _, _)    => Seq(s"$prefix$name")
    }
  }

  def extractArrayColsAll(schema: StructType, prefix: String): Seq[String] =
    schema.fields.flatMap {
      case StructField(name, struct: StructType, _, _)           => extractArrayColsAll(struct, prefix + name + ".")
      case StructField(name, ArrayType(_, _), _, _)              => Seq(s"$prefix$name")
//      case StructField(name, ArrayType(ss: StructType, _), _, _) => extractArrayColsAll(ss, "")
    }

  def extractArrayColsT(schema: StructType, prefix: String): Seq[String] = {
    schema.fields.flatMap {
      case StructField(name, struct: StructType, _, _)           => extractArrayColsT(struct, prefix + name + ".")
      case StructField(name, ArrayType(_, _), _, _)              => Seq(s"$prefix$name")
//      case StructField(name, ArrayType(ss: StructType, _), _, _) => ss.fields.flatMap { case StructField(name, struct: StructType, _, _) => extractArrayColsT(struct, prefix + name + ".") }
    }
  }

  def fieldsAll(schema: StructType, prefix: String): Seq[String] = {
    val fullName: String => String = name => if (prefix.isEmpty) name else s"$prefix.$name"

    schema.fields.flatMap {
      case StructField(name, struct: StructType, _, _) => fullName(name) +: fieldsAll(struct, fullName(name))
      case StructField(name, ArrayType(struct: StructType, _), _, _) => fullName(name) +: fieldsAll(struct, fullName(name))
      case _ => Seq("NãoHaMaisNada") //Seq.empty[String]
    }
  }

  def fieldsFull(schema: StructType, prefix: String): Seq[String] = {
    val fullName: String => String = name => if (prefix.isEmpty) name else s"$prefix.$name"

    schema.fields.flatMap {
      case StructField(name, struct: StructType, _, _)               => fullName(name) +: fieldsFull(struct, fullName(name))
      case StructField(name, ArrayType(struct: StructType, _), _, _) => struct.fields.flatMap { case StructField(name, _, _, _) => fullName(name) +: fieldsFull(struct, fullName(name)) }
      case StructField(name, _, _, _)                                => Seq(fullName(name))
    }
  }

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

  def udfCol(df: DataFrame, fields: Seq[String]): Seq[DataFrame] = {
    fields.map(f => df.withColumn(f + "exploded", explode(df(f))).drop(f)).toSeq
  }

  def udfColumns(df: DataFrame, fields: String): Seq[DataFrame] = {

    Seq(df.withColumn(fields + "exploded", explode(df(fields))))
  }

  def udfColm(df: DataFrame, fields: Seq[String]): Seq[DataFrame] = {
    fields.map(f => df.withColumn(f, explode(df(f)))).toSeq
  }

  def udfId(df: Seq[DataFrame]): Seq[DataFrame] = {
    df.map(f => f.withColumn("index", monotonically_increasing_id)).toSeq
  }

  def AllZip(col: List[Seq[String]]) {
    val c = col.map(x => x)
  }

  def udfSeq(df: DataFrame, fields: Seq[String]): Seq[DataFrame] = {
    val colindex = "index"

    fields match {
      case Seq(x) => Seq(df.withColumn(x, explode(df(x))))
      case last   => Seq(df.withColumn(colindex, lit("")))
    }
  }

  val o = List(Seq(("a", "b", "c", "d"), ("a", "b", "c", "d")))

  def fieldsArr(schema: StructType, prefix: String): Seq[String] = {
    val fullName: String => String = name => if (prefix.isEmpty) name else s"$prefix.$name"

    schema.fields.flatMap {
      case StructField(name, struct: StructType, _, _) => fieldsArr(struct, "")
      case StructField(name, _, _, _)                  => Seq.empty[String]
//      case StructField(name, ArrayType(_, _), _, _)    => Seq(name)

    }
  }

  //case StructField(name, ArrayType(struct: StructType, _), _, _) => fieldsFinal(struct, fullName(name))
  //Em cima basicmente mando o struct em questão, chamando novamente a função fieldFinal que automaticamente cairá em outro case - Em baixo
  //case StructField(name, _, _, _)                 => Seq(fullName(name))

  //ss.fields map { s =>  fullName(s.name) }
  def findFields(path: String, dt: DataType) {
    val x = dt match {
      case s: StructType => s.fields.foreach(f => findFields(path + f.name + ".", f.dataType))
      case other         => println(s"$path")
    }

    def findArrayTypes(parents: Seq[String], f: StructField): Seq[String] = {
      f.dataType match {
        case array: ArrayType   => parents
        case struct: StructType => struct.fields.toSeq.map(f => findArrayTypes(parents :+ f.name, f)).flatten
        case _                  => Seq.empty[String]
      }
    }

    //    def fields(df: DataFrame, c: String) = df.schema(c) match {
    //      case StructField(_, ArrayType(ArrayType(ss: StructType, _), _), _, _) =>
    //        ss.fields map { s =>
    //          (s.name, s.dataType)
    //        }
    //    }

    //    def fields(df: DataFrame, c: String) = df.schema(c) match {
    //      case StructField(_, ArrayType(ArrayType(ss: StructType, _), _), _, _) =>
    //        ss.fields map { s =>
    //          (s.name, s.dataType)
    //        }
    //    }

    def findFieldsFull(path: String, dt: DataType): Unit = dt match {
      case s: StructType =>
        s.fields.foreach(f => print(f.name))
      case other => println(s"$path")
    }
  }
  //  def flattenAAA(schema: StructType): Array[StructField] = schema.fields.flatMap { f =>
  //  f.dataType match {
  //    case struct: StructType => flatten(struct)
  //    case _ => Array(f)
  //  }
}

object maptodf extends App {

  val jsonString = """$daa355f4-a6d6-429d-be71-d72dcf992549aa:{"glossary": {"title": "example glossary","GlossDiv": {"title": "S","GlossList": {"GlossEntry": {"ID": "SGML","SortAs": "SGML","GlossTerm": "Standard Generalized Markup Language","Acronym": "SGML","Abbrev": "ISO 8879:1986","GlossDef": {"para": "A meta-markup language, used to create markup languages such as DocBook.","GlossSeeAlso": ["GML", "XML"]},"GlossSee": "markup"}}}}}"""
  val rdd = jsonString.slice(40, 600)
  JSON.parseFull(rdd)
}


   
   //  val row = Row.fromSeq(json)
//  print(row)
//  val rows = json.map { x => Row(x) }
//  val rows = json.map { x => print(x) }
  

 