package com.igorvovk.eaglepeak

import java.io.File

import breeze.linalg.Matrix
import com.google.inject.Guice
import com.igorvovk.eaglepeak.domain.Descriptor
import com.igorvovk.eaglepeak.domain.Identifiable._
import com.igorvovk.eaglepeak.guice.{ConfigModule, SparkModule}
import com.igorvovk.eaglepeak.math.{CommonOperations, ComparingByContinuousProperties, ComparingByDiscreteProperties}
import com.igorvovk.eaglepeak.service.IO
import net.codingwell.scalaguice.InjectorExtensions._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

import scala.io.StdIn

object Application extends App {

  println("Hello from Eagle Peek!")

  val injector = Guice.createInjector(
    new ConfigModule,
    new SparkModule()
  )

  val sc = injector.instance[SparkContext]
  val sqlc = injector.instance[SQLContext]

  val fixture = Fixtures.athlettes(sqlc)
  fixture.persist(StorageLevel.MEMORY_AND_DISK)

  println("Schema:")
  fixture.printSchema()

  def buildSimilarities(path: String) = {
    import com.igorvovk.eaglepeak.math.CommonOperations._

    val discretePropsComparator = new ComparingByDiscreteProperties[String]
    val continuousPropsComparator = new ComparingByContinuousProperties[String]()

    val athleteDescriptors = buildDescriptors[String](fixture, "Athlete")

    val matrices = Map(
      "Country" -> discretePropsComparator.compare(groupBy(fixture, "Athlete", "Country")._1).matrix,
      "Sport" -> discretePropsComparator.compare(groupBy(fixture, "Athlete", "Sport")._1).matrix,
      "Year" -> continuousPropsComparator.compare(groupBy[String, String](fixture, "Athlete", "Year")._1.mapValues(s => java.lang.Double.valueOf(s.head))).matrix
    )

    val transposed = rotateAndTranspose(matrices.values.toSeq)
//    val filtered = filterSelfIndices(transposed)

    IO.saveRDD(athleteDescriptors, s"$path/descriptors")

    IO.saveRDD(transposed, s"$path/matrix")
  }

  def loadSimilarities(path: String) = {
    val descriptors = IO.loadRDD[Descriptor[String]](sc, s"$path/descriptors").collect().toSeq

    println("Descriptors loaded")

//    val matrices = descriptors.par.flatMap(d => {
//      val rddPath = s"$path/matrix/${d.id}"
//      if (Files.exists(Paths.get(rddPath))) {
//        try {
//          val rows = IO.loadRDD[IndexedRow](sc, rddPath)
//
//          val m = new IndexedRowMatrix(rows)
//          // Cache
////          m.numRows()
////          m.numCols()
//
//          Iterator.single(d.id -> m)
//        } catch {
//          case e: Throwable =>
//            println(s"Failed to load RDD $rddPath")
//
//            Iterator.empty
//        }
//      } else {
//        Iterator.empty
//      }
//    }).seq.toMap

    val matrix = IO.loadRDD[(Int, Matrix[Double])](sc, s"$path/matrix")

    println("Matrices loaded")

    (descriptors, matrix)
  }

  val path = "/tmp/bootsrap"
  if (!new File(path).exists()) {
    println("Start building similarities")
    buildSimilarities(path)
    println("Building similarities finished")
  }


  println("Loading similarities")
  val (descriptors, similarities) = loadSimilarities(path)

  val descriptorsByDescription = descriptors.map(_.tupleInv).toMap
  val descriptorsById = descriptors.map(_.tuple).toMap

  val from = Set(
    301
  )

  println("From: " + from.map(descriptorsById))

  CommonOperations.similar(similarities)(from).slice(0, 50).foreach(row => {
    println(descriptorsById(row.id) + ": " + row.value)
  })

  println("Done")


  StdIn.readLine("Enter to exit...")

  //...

  sys.addShutdownHook {
    sc.stop()
  }

}
