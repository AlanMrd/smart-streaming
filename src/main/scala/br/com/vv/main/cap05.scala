package br.com.vv.main

object Demo extends App{
    val topics = Array("TESTE_STREAMING", "TESTE_STREAMING1")
    val x = "TESTE_STREAMING1"
  
   val results =  x.find(_ == topics) match{
      case Some(x) => x
      case _ => null
  }
    
    print(results)
}