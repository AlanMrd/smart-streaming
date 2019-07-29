package br.com.vv.utils

object GeneralUtil {
  def getWrapParameter(parameter: Option[String]): String = {
    parameter match {
      case Some(e: String) => e
      case None            => throw new Exception("Classe ou tabela não existe!!!")
    }
  }
}