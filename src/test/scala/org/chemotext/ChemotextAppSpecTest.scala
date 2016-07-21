package org.chemotext;

import org.apache.commons.io.FileUtils
import java.io.File
import java.nio.file.{Paths, Files}
import org.scalatest._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.holdenkarau.spark.testing.SharedSparkContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.json4s._
import collection.immutable.HashMap

// https://github.com/holdenk/spark-testing-base
@RunWith(classOf[JUnitRunner])
class ChemotextAppSpecTest extends FunSuite with SharedSparkContext {

  val logger = LoggerFactory.getLogger("ChemotextAppSpec")

  val dataHome = "./data"
  val ctdHome = s"$dataHome/ctd"
  val outputDir = s"$dataHome/test/output"
  val articlePath = s"$dataHome/pubmed"

  val expect = HashMap (
    s"$outputDir/test-1.fxml.json" -> HashMap (
      "AB" -> Array (
        // Verify that these binaries are in the AB list.
        Array ("10,10-bis(4-pyridinylmethyl)-9(10h)-anthracenone", "kcnq1"),
        Array ("10,11-dihydro-10,11-dihydroxy-5h-dibenzazepine-5-carboxamide", "ephx1")
      ),
      "BC" -> Array (
        // Verify that this binary is corroborated by CTD with the supplied reference PMID.
        Array ("11-beta-hsd3", "hypokinesia", "24394944"),
        Array ("11-beta-hsd3", "lymphocytosis", "11915957")
      ),
      "ABC" -> Array (
        // Verify that these (A/B/C) triples are identified
        Array ("10,10-bis(4-pyridinylmethyl)-9(10h)-anthracenone", "kcnq1", "diabetes mellitus, type 2"),
        Array ("10,11-dihydro-10,11-dihydroxy-5h-dibenzazepine-5-carboxamide", "ephx1", "osteoarthritis")
      )
    ),
    s"$outputDir/test-2.fxml.json" -> HashMap (
      "AB" -> Array (
        Array ("10,10-bis(4-pyridinylmethyl)-9(10h)-anthracenone", "kcnq2")
      )
    ),
    s"$outputDir/test-3.fxml.json" -> HashMap (
      "AB" -> Array (
        Array ("10,10-bis(4-pyridinylmethyl)-9(10h)-anthracenone", "kcnq2"),
        Array ("10,10-bis(4-pyridinylmethyl)-9(10h)-anthracenone", "kcnq3")
      )
    ),
    s"$outputDir/test-4.fxml.json" -> HashMap ( "AB" -> Array[Array[String]]() )
  )

  implicit val formats = DefaultFormats

  test ("Article analysis pipeline.") {

    val output = new File (outputDir)
    if (Files.exists(Paths.get(outputDir))) {
      FileUtils.deleteDirectory (output)
    }
    output.mkdirs ()

    ChemotextApp.main (Array(
      "--name",              "CT2-TEST",
      "--dataHome",          s"$dataHome",
      "--articles",          articlePath,
      "--mesh",              s"$dataHome/mesh/desc2016_2.xml",
      "--chemlex",           "mesh",
      "--sample",            "1",
      "--ctdAC",             s"$ctdHome/CTD_chemicals_diseases.csv",
      "--ctdAB",             s"$ctdHome/CTD_chem_gene_ixns.csv",
      "--ctdBC",             s"$ctdHome/CTD_genes_diseases.csv",
      "--output",            outputDir,
      "--slices",            "1",
      "--lexerConfig",       s"$dataHome/tmchem/config/banner_JOINT_dev.xml",
      "--lexerCache",        s"$dataHome/cacheFile.tsv",
      "--lexerDict",         s"$dataHome/data/dict.txt",
      "--distanceThreshold", "800"
    ), sc)

  }

  test ("Verify output binaries and triples.") {
    val options = JSONUtils.readJSON (s"$outputDir/chemotextOpts.json").extract [ChemotextConfig]
    assert (options.articlePath == articlePath)
    for ( (k, v) <- expect ) {
      println (s"[ $k ]:")
      val article = getOutput (k)
      v.get ("AB").foreach { pairs =>
        assert (countMatches (pairs, article.AB, "AB") == pairs.length)
      }
      v.get ("BC").foreach { pairs =>
        assert (countMatches (pairs, article.BC, "BC") == pairs.length)
      }
      v.get ("AC").foreach { pairs =>
        assert (countMatches (pairs, article.AC, "AC") == pairs.length)
      }
      v.get ("ABC").foreach { triples =>
        assert (countTripleMatches (triples, article.ABC, "ABC") == triples.length)
      }
    }
  }

  // Verify that we eliminate duplicate binaries in favor of instances with the shortest distance.
  test ("Verify elimination of duplicate binaries") {
    val article = getOutput (s"$outputDir/test-4.fxml.json")
    var count = 0
    for (p <- article.AB) {
      if (p.L == "10,10-bis(4-pyridinylmethyl)-9(10h)-anthracenone" && p.R == "kcnq1") {
        count += 1
      }
    }
    assert (count == 1)
    println (s"Validated elimniation of duplicate pairs.")
  }

  def getOutput (n : String) = JSONUtils.readJSON (n).extract[ChemotextProcessor.QuantifiedArticle]
 
  def countMatches (
    pairs    : Array[Array[String]],
    binaries : List[ChemotextProcessor.Binary],
    kind     : String) = 
  {
    var matchCount = 0
    for (p <- binaries) {
        for (pair <- pairs) {
          //println (s" -- Comparing $kind: ${p.L} -> ${p.R} =?= ${pair(0)} -> ${pair(1)}")
          if (p.L == pair(0) && p.R == pair(1)) {
            println (s" -- Matched $kind: ${p.L} -> ${p.R}")
            if (pair.length == 3) {
              if (p.fact) {
                if (p.refs.contains (pair (2))) {
                  println (s" -- Matched PubMed ID reference in binary extracted from CTD: ${pair(2)}")
                  matchCount += 1
                } else {
                  // a ref is expected but not matched for this fact.
                }
              }
            } else {
              // not expecting a fact reference so we're done matching.
              matchCount += 1
            }
          }
        }
    }
    matchCount
  }

  def countTripleMatches (
    expected : Array[Array[String]],
    triples  : List[ChemotextProcessor.Triple],
    kind     : String) =
  {
    var matchCount = 0
    for (p <- triples) {
      for (exp <- expected) {
        //println (s" -- Comparing $kind: ${p.A} / ${p.B} / ${p.C} =?= ${exp(0)} / ${exp(1)} / ${exp(2)}")
        if (p.A == exp(0) && p.B == exp(1) && p.C == exp(2)) {
          println (s" -- Matched $kind: ${p.A} / ${p.B} / ${p.C}")
          matchCount += 1
        }
      }
    }
    matchCount
  }

}
