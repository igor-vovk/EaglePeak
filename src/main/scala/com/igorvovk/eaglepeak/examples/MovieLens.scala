package com.igorvovk.eaglepeak.examples

import java.io.File
import java.net.URL
import breeze.linalg.{SparseVector, Matrix}
import breeze.util.{Profiling, Index}
import com.google.inject.Guice
import com.igorvovk.eaglepeak.examples.SimilarAthlettes._
import com.igorvovk.eaglepeak.guice.{KryoModule, SparkModule, ConfigModule}
import com.igorvovk.eaglepeak.io.Serialization
import com.igorvovk.eaglepeak.math.comparators.{ContinuousPropertiesComparator, DiscretePropertiesComparator}
import com.igorvovk.eaglepeak.math.recommendations.RawFirst
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

import net.codingwell.scalaguice.InjectorExtensions._

import com.igorvovk.eaglepeak.service.Logging

import scala.io.StdIn
import scala.util.Random
import scala.util.control.NonFatal

object MovieLens extends App with Logging {

//  def downloadUrl(small: Boolean) = {
//    val part = if (small) "-small" else ""
//
//    s"http://files.grouplens.org/datasets/movielens/ml-latest$part.zip"
//  }
//
//  def downloadAndExtract(url: String, dest: File) = {
//    require(dest.exists() && dest.isDirectory)
//    debug(s"Starting downloading $url")
//
//    val tmp = File.createTempFile("movielens", ".zip")
//
//    new URL(url) #> tmp !!
//  }

  val injector = Guice.createInjector(
    new ConfigModule,
    new SparkModule(),
    new KryoModule
  )

  val ser = injector.instance[Serialization]


  def buildSimilarities(path: String): Unit = {
    import com.igorvovk.eaglepeak.math.CommonOperations._

    val sqlc = injector.instance[SQLContext]

    trace("Loading dataset...")
    val fixture = Fixtures.movielens(sqlc, "ml-latest") //.sample(withReplacement = false, 0.1)

    val discretePropsComparator = new DiscretePropertiesComparator

    ser.save(mkIndex[String](fixture.movies, "movieId"), s"$path/index")

    val matrices = Map(
      "genres" -> discretePropsComparator.compare(
        extractDiscreteProps[String, String](fixture.movies, "movieId", "genres", _.split("|"))._1.values
      ).matrix
    )

    val transposed = rotateAndTranspose(matrices.values.toSeq)

    transposed.toLocalIterator.foreach { case (objId, matrix) =>
      ser.save(matrix, s"$path/matrix/$objId.matrix")
    }
  }

  def loadSimilarities(path: String) = {
    val index = ser.load[Index[String]](s"$path/index")

    debug("Descriptors loaded")

    val getMatrix: Int => Option[Matrix[Double]] = id => {
      debug(s"Loading matrix $id")

      try {
        Some(ser.load[Matrix[Double]](s"$path/matrix/$id.matrix"))
      } catch {
        case NonFatal(e) =>
          warn("Error loading matrix", e)
          None
      }
    }

    val mem = Array.tabulate(index.size)(getMatrix)

    (index, mem)
  }

  val path = "/tmp/movielens"
  if (!new File(path).exists()) {
    debug("Start building similarities")
    buildSimilarities(path)
    debug("Building similarities finished")
  }


//  debug("Loading similarities")
//  val (idx, storage) = loadSimilarities(path)
//
//  val rand = new Random()
//  val recommendationsAlgo = new RawFirst(storage, idx.size, 3)
//
//  def loop() = {
//    rand.setSeed(System.currentTimeMillis())
//    val from = SparseVector.zeros[Double](idx.size)
//    for (_ <- 0 to 30) {
//      from(rand.nextInt(idx.size)) = 1.0d
//    }
//
//    debug("From: " + from.activeKeysIterator.map(idx.get).mkString(", "))
//
//    recommendationsAlgo.similar(from, limit = 50).foreach(row => {
//      //      println(idx.get(row.id) + ": " + row.value)
//    })
//  }
//
//  info("Recommendations fetch time: " + Profiling.time(100)(loop) + " ms")

  StdIn.readLine("Enter to exit...")
}
