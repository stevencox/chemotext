package org.chemotext

/**
  *  Track a place in a document
  */
case class Position (
  var document  : Int = 0,
  var text      : Int = 0,
  var paragraph : Int = 0,
  var sentence  : Int = 0
)
