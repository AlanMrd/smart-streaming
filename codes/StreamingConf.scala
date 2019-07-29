package br.com.vv.vo
import br.com.vv.utils.BusinessTemplate

class StreamingConf(nm_class: Option[String], nm_table: Option[String]) extends Serializable {
  val _nm_class: String = getWrapParameter(nm_class)
  val _nm_table: String = getWrapParameter(nm_table) 
  val _instance: BusinessTemplate = StreamingConfig.getInstance
  
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
      println(instance)
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
   

