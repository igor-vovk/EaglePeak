package com.igorvovk.eaglepeak

import java.io.File

import breeze.linalg.Matrix
import com.google.inject.Guice
import com.igorvovk.eaglepeak.domain.Descriptor
import com.igorvovk.eaglepeak.domain.Descriptor.DescriptorId
import com.igorvovk.eaglepeak.guice.{ConfigModule, SparkModule}
import com.igorvovk.eaglepeak.math.comparators.{ContinuousPropertiesComparator, DiscretePropertiesComparator}
import com.igorvovk.eaglepeak.math.recommendations.RawFirst
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

    val discretePropsComparator = new DiscretePropertiesComparator[DescriptorId]
    val continuousPropsComparator = new ContinuousPropertiesComparator

    val athleteDescriptors = buildDescriptors[String](fixture, "Athlete")

    val matrices = Map(
      "Country" -> discretePropsComparator.compare(groupBy[String, String](fixture, "Athlete", "Country")._1.values).matrix,
      "Sport" -> discretePropsComparator.compare(groupBy[String, String](fixture, "Athlete", "Sport")._1.values).matrix,
      "Year" -> continuousPropsComparator.compare(groupBy[String, String](fixture, "Athlete", "Year")._1.values.map(s => java.lang.Double.valueOf(s.head))).matrix
    )

    val transposed = rotateAndTranspose(matrices.values.toSeq)
//    val filtered = filterSelfIndices(transposed)

    IO.saveRDD(athleteDescriptors, s"$path/descriptors")

    IO.saveRDD(transposed, s"$path/matrix")
  }

  def loadSimilarities(path: String) = {
    val descriptors = IO.loadRDD[Descriptor[String]](sc, s"$path/descriptors").collect().toSeq

    println("Descriptors loaded")

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

  val recommendationsAlgo = new RawFirst(similarities)

  recommendationsAlgo.similar(from).slice(0, 50).foreach(row => {
    println(descriptorsById(row.id) + ": " + row.value)
  })

  println("Done")


  StdIn.readLine("Enter to exit...")

  //...

  sys.addShutdownHook {
    sc.stop()
  }

}
