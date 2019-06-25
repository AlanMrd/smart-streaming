package br.com.vv.main

import br.com.vv.dao.hbaseDAO
import br.com.vv.utils._
import org.apache.spark.sql.functions._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.SparkSession
import br.com.vv.dao.hbaseConnection.getTable

class Processor extends Serializable {
  def processStream(ss: SparkSession, stream: DStream[String]) {

    val c = new GeneratorSchema()
    val h = new hbaseDAO();

    stream.foreachRDD { frdd =>
      val df_json = ss.read.option("multiLine", true).json(frdd)
      val cols = c.fieldsFinal(df_json.schema, "")
      val cols_renam = cols.map(x => x.replace(".", "_"))

      val column_names_col = cols.map(name => col(name))
      val df = df_json.select(column_names_col: _*)
      val df_new = df.toDF(cols_renam: _*)

      val df_final = c.containsArray(c.hasArray(df_new), df_new).toDF(cols_renam: _*)
      df_final.show()

      //      df_final.foreachPartition{f =>
      //        val tb = getTable("smartcommerce:")
      //        print(tb)
      //
      //        f.foreach(g => h.putOnHbase(g))}

    }

  }

  def processStreamOne(ss: SparkSession, stream: DStream[ConsumerRecord[String, String]]) {

    val c = new GeneratorSchema()
    val h = new hbaseDAO();
   
//    val p = stream.map(f => f.topic.)
//    val t = stream.map(f => f.value())
//
//    t.foreachRDD { frdd =>
//      
//      val df_json = ss.read.option("multiLine", true).json(frdd)
//      val cols = c.fieldsFinal(df_json.schema, "")
//      val cols_renam = cols.map(x => x.replace(".", "_"))
//
//      val column_names_col = cols.map(name => col(name))
//      val df = df_json.select(column_names_col: _*)
//      val df_new = df.toDF(cols_renam: _*)
//
//      val df_final = c.containsArray(c.hasArray(df_new), df_new).toDF(cols_renam: _*)
//      df_final.show()
//
//      df_final.foreachPartition { f =>
//        val tb = getTable("smartcommerce:"+p)
//        f.foreach(g => h.putOnHbase(g))
//      }
//    }

    //    //        f.foreach(g => h.putOnHbase(g))}
    //
    //    //    }

        stream.foreachRDD { rdd =>
          val topic = rdd.map(f => f.topic().toString())
          val value = rdd.map(f => f.value())
          
          
            val df_json = ss.read.option("multiLine", true).json(value)
            val cols = c.fieldsFinal(df_json.schema, "")
            val cols_renam = cols.map(x => x.replace(".", "_"))
    
            val column_names_col = cols.map(name => col(name))
            val df = df_json.select(column_names_col: _*)
            val df_new = df.toDF(cols_renam: _*)
    
            val df_final = c.containsArray(c.hasArray(df_new), df_new).toDF(cols_renam: _*)
            df_final.show()
    
            df_final.foreachPartition { f =>
              val tb = getTable("smartcommerce:"+topic.take(1))
              print(tb)
    
              f.foreach(g => h.putOnHbase(g))
            }
    
          }
//        }

    //    t.foreachRDD { frdd =>
    //      val df_json = ss.read.option("multiLine", true).json(frdd)
    //      val cols = c.fieldsFinal(df_json.schema, "")
    //      val cols_renam = cols.map(x => x.replace(".", "_"))
    //
    //      val column_names_col = cols.map(name => col(name))
    //      val df = df_json.select(column_names_col: _*)
    //      val df_new = df.toDF(cols_renam: _*)
    //
    //      val df_final = c.containsArray(c.hasArray(df_new), df_new).toDF(cols_renam: _*)
    //      df_final.show()

    //      df_final.foreachPartition{f =>
    //        val tb = getTable("smartcommerce:")
    //        print(tb)
    //
    //        f.foreach(g => h.putOnHbase(g))}

    //    }

  }

}