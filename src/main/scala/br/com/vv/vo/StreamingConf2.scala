package br.com.vv.vo
import br.com.vv.utils.BusinessTemplate

class StreamingConf2(nm_class: String, nm_table: String) extends Serializable {
  val _nm_class: String = nm_class
  val _nm_table: String = nm_table 
  val _connection: BusinessTemplate = StreamingConfig.getInstance
  
  def getWrapParameter(parameter:Option[String]): String = {
    parameter match{
      case Some(e:String) => e
      case None => throw new Exception("Classe ou tabela não existe!!!")
    }
  }

  object StreamingConfig extends Serializable {
    def getInstance(): BusinessTemplate = {
      val cl = Class.forName(_nm_class).asInstanceOf[Class[_ <: br.com.vv.utils.BusinessTemplate]]
      lazy val instance = cl.newInstance()
//      println(instance)
      instance
    }
  }
  
  

  
  
//  object StreamingConfig extends Serializable {
//    def getInstance(): BusinessTemplate = {
//      val classe = _nm_class match{
//        case Some(e:String) => e
//        case None => throw new Exception("Classe não existe!!!")
//      }
//      
//      val cl = Class.forName(classe).asInstanceOf[Class[_ <: br.com.vv.utils.BusinessTemplate]]
//      lazy val instance = cl.newInstance()
//      println(instance)
//      instance
//    }
//  }

}
   

