package br.com.vv.main

import br.com.vv.utils._

object host extends App{
  
  val conf = new Config()
  val streamConfig = conf.getConfStreaming()
  
  print(streamConfig)
  
}