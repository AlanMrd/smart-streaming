package br.com.vv.host

import br.com.vv.dao.EventManager

object Event extends App{
  val event = new EventManager()
  event.addSubscribers()
  
  event.show()
}