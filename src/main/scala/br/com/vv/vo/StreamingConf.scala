package br.com.vv.vo
import br.com.vv.utils.BusinessTemplate

class StreamingConf(nm_class: String, nm_table: String) extends Serializable {
  val _nm_class: String = nm_class
  val _nm_table: String = nm_table
  val _connection: BusinessTemplate = StreamingConf.getInstance

  object StreamingConf extends Serializable {
    def getInstance(): BusinessTemplate = {
      val classe = Class.forName(_nm_class).asInstanceOf[Class[_ <: br.com.vv.utils.BusinessTemplate]]
      lazy val instance = classe.newInstance()
      println(instance)
      instance
    }
  }
}
   

