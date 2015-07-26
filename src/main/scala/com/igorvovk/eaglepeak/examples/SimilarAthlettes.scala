package com.igorvovk.eaglepeak.examples

import java.io.File

import breeze.linalg.{Matrix, SparseVector}
import breeze.util.{Index, Profiling}
import com.google.inject.Guice
import com.igorvovk.eaglepeak.guice.{ConfigModule, KryoModule, SparkModule}
import com.igorvovk.eaglepeak.io.Serialization
import com.igorvovk.eaglepeak.math.comparators.{ContinuousPropertiesComparator, DiscretePropertiesComparator}
import com.igorvovk.eaglepeak.math.recommendations.RawFirst
import com.igorvovk.eaglepeak.service.Logging
import net.codingwell.scalaguice.InjectorExtensions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

import scala.io.StdIn
import scala.util.Random
import scala.util.control.NonFatal

object SimilarAthlettes extends App with Logging {

  info("Hello from Eagle Peek!")

  val injector = Guice.createInjector(
    new ConfigModule,
    new SparkModule(),
    new KryoModule
  )

  val ser = injector.instance[Serialization]

  def buildSimilarities(path: String) = {
    import com.igorvovk.eaglepeak.math.CommonOperations._

    val sqlc = injector.instance[SQLContext]

    val fixture = Fixtures.athlettes(sqlc)
    fixture.persist(StorageLevel.MEMORY_AND_DISK)

    debug("Schema: " + fixture.schema.treeString)

    val discretePropsComparator = new DiscretePropertiesComparator
    val continuousPropsComparator = new ContinuousPropertiesComparator

    val athleteIndex = index[String](fixture, "Athlete")

    val matrices = Map(
      "Country" -> discretePropsComparator.compare(extractDiscreteProps[String, String](fixture, "Athlete", "Country")._1.values).matrix,
      "Sport" -> discretePropsComparator.compare(extractDiscreteProps[String, String](fixture, "Athlete", "Sport")._1.values).matrix,
      "Year" -> continuousPropsComparator.compare(groupBy[String, String](fixture, "Athlete", "Year")._1.values.map(s => java.lang.Double.valueOf(s.head))).matrix
    )

    val transposed = rotateAndTranspose(matrices.values.toSeq)

    ser.save(athleteIndex, s"$path/index")

    transposed.foreach { case (objId, matrix) =>
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

  val path = "/tmp/athlettes"
  if (!new File(path).exists()) {
    debug("Start building similarities")
    buildSimilarities(path)
    debug("Building similarities finished")
  }


  debug("Loading similarities")
  val (idx, storage) = loadSimilarities(path)

  val rand = new Random()
  val recommendationsAlgo = new RawFirst(storage, idx.size, 3)

  def loop() = {
    rand.setSeed(System.currentTimeMillis())
    val from = SparseVector.zeros[Double](idx.size)
    for (_ <- 0 to 30) {
      from(rand.nextInt(idx.size)) = 1.0d
    }

    debug("From: " + from.activeKeysIterator.map(idx.get).mkString(", "))

    recommendationsAlgo.similar(from, limit = 50).foreach(row => {
//      println(idx.get(row.id) + ": " + row.value)
    })

//    debug("Done")
  }

  println("Recommendations fetch time: " + Profiling.time(100)(loop) + " ms")

  StdIn.readLine("Enter to exit...")

  //...

}
