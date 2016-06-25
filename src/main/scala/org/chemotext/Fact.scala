package org.chemotext

/**
  * Represenation of known propositions from authoratative databases.
  */
case class Fact (
  L     : String,
  R     : String,
  code  : Int,
  PMIDs : Array[String]
)
