package br.com.vv.dao

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.PrintWriter;

class hdfsDAO {
  def insertHDFS(){
   
  println("Trying to write to HDFS...")
  val conf = new Configuration()
  conf.set("fs.defaultFS", "hdfs://vvdataprdnnha")

  val fs = FileSystem.get(conf)
  val output = fs.create(new Path("/tmp/mySample.txt"))
  
  val writer = new PrintWriter(output)
  try {
    writer.write("this is a test")
    writer.write("\n")
  } finally {
    writer.close()
  }
  println("Done!")
  }
  
  
}
