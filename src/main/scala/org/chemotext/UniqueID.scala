package org.chemotext

object UniqueID {
  private var id : Long = 0
  def inc () = { id += 1; id }
}
