package br.com.vv.main

import scala.util.matching.Regex 

object matchString extends App {
   val fullField = Seq("glossary","glossary.GlossDiv","glossary.GlossDiv.GlossList","glossary.GlossDiv.GlossList.GlossEntry","glossary.GlossDiv.GlossList.GlossEntry.Abbrev","glossary.GlossDiv.GlossList.GlossEntry.Acronym","glossary.GlossDiv.GlossList.GlossEntry.GlossDef","glossary.GlossDiv.GlossList.GlossEntry.GlossDef.GlossSeeAlso","glossary.GlossDiv.GlossList.GlossEntry.GlossDef.para","glossary.GlossDiv.GlossList.GlossEntry.GlossSee","glossary.GlossDiv.GlossList.GlossEntry.GlossTerm","glossary.GlossDiv.GlossList.GlossEntry.ID","glossary.GlossDiv.GlossList.GlossEntry.SortAs","glossary.GlossDiv.title","glossary.title")
   
   val fields = Array("Abbrev", "Acronym", "GlossSeeAlso", "para", "GlossSee", "GlossTerm", "ID", "SortAs", "title12", "title")
   
   val a = Array("Alan","Junior")
   val b = "a"
   
   for(f <- fields)
    print(f)
     
      
}