package org.chemotext

import java.io.PrintStream

/**
  * Defeat the fact that the version of banner used by tmChem uses System.out for logging.
  * There's no way to configure it, so we'll intercept System.out and turn it off.
  * Without this, processes fill up the disks on cluster worker nodes, killing the job.
  */
class BannerFilterPrintStream (stream: PrintStream) extends PrintStream (stream) {  
  override def println (s : String) {
    if (! s.startsWith ("WARNING:") && s.contains ("lists no concept")) {
      super.print (s)
    }
  }
}
