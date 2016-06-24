package org.chemotext

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.xml.sax.SAXParseException
import scala.xml.XML

class PubMedArticleParser ( article : String ) {

  val logger = LoggerFactory.getLogger("PubMedArticleParser")

  val xml = XML.loadFile (article)

  def getParagraphs () = {
    (xml \\ "p")
  }

  def getId () = {
    val pmid = (xml \\ "article-id").filter { node =>
      node.attribute ("pub-id-type").exists { pubIdType =>
        pubIdType.text == "pmid"
      }
    }.text
    pmid
  }

  def getDate () = {
    val date = (xml \\ "pub-date").filter { node =>
      node.attribute ("pub-type").exists { pubType =>
        pubType.text == "epub"
      }
    }
    val day = (date \\ "day").text
    val month = (date \\ "month").text
    val year = (date \\ "year").text
    s"$day-$month-$year".trim ()
  }

}

