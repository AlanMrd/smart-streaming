package br.com.vv.vo

import br.com.vv.utils.BusinessTemplate
import br.com.vv.events.EventManager

class ConfigStreaming(topic: String, nm_class: String, sink: List[Sink]) {
  val _topic: String = topic
  val _nm_class: String = nm_class
  val _sink: List[Sink] = sink
  val _instance: BusinessTemplate = ConfigStreaming.getInstance
  val _event = new EventManager(_sink)

  object ConfigStreaming extends Serializable {
    def getInstance(): BusinessTemplate = {
      val cl = Class.forName(_nm_class).asInstanceOf[Class[_ <: br.com.vv.utils.BusinessTemplate]]
      lazy val instance = cl.newInstance()
      instance
    }
  }
}
