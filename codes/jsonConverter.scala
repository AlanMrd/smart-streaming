package br.com.vv.host

import br.com.vv.utils.Converter

object jsonConverter extends App {
//  val jsonString = """$daa355f4-a6d6-429d-be71-d72dcf992549�:{"Compra": {"IdCompraEntrega": 73665799,"IdCompra": 164379233,"IdFreteEntregaTipo": 1}}"""
  val jsonString = """$daa355f4-a6d6-429d-be71-d72dcf992549aa:{"glossary": {"title": "example glossary","GlossDiv": {"title": "S","GlossList": {"GlossEntry": {"ID": "SGML","SortAs": "SGML","GlossTerm": "Standard Generalized Markup Language","Acronym": "SGML","Abbrev": "ISO 8879:1986","GlossDef": {"para": "A meta-markup language, used to create markup languages such as DocBook.","GlossSeeAlso": ["GML", "XML"]},"GlossSee": "markup"}}}}}"""
//  val jsonString = """$daa355f4-a6d6-429d-be71-d72dcf992549�:{"IdCompraEntrega": 1,"IdCompra": 2,"IdFreteEntregaTipo": 3}"""
//  val jsonString = """{"Compra": {"IdCompraEntrega": 73665799,"IdCompra": 164379233,"IdFreteEntregaTipo": 1}}"""
  val c = new Converter()
  val o = c.jsonToClass(jsonString)
  print(o)
}
