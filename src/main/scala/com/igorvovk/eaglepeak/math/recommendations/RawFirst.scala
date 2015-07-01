package com.igorvovk.eaglepeak.math.recommendations

import breeze.linalg.{DenseVector, Matrix}
import com.igorvovk.eaglepeak.domain.Descriptor._
import com.igorvovk.eaglepeak.domain.{Identifiable, IdentifiableDouble}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * First implementation of merging and sorting algorithm. Did not use any caches, but support scoring.
 * Good for small amounts of input data.
 *
 *
 * @param storage Map, in which keys are identifiers of objects to start, and values are matrices with similarities.
 *                Rows are described as different objects, and columns as different properties
 */
class RawFirst(storage: RDD[(DescriptorId, Matrix[Double])]) {

  storage.persist(StorageLevel.MEMORY_AND_DISK)

  val cols = storage.first()._2.cols

  /**
   *
   * @param start Find similarities for this objects
   * @param coeff Coefficients applied to properties in matrix (rows in storage), default coefficient is 1
   * @return
   */
  def similar(start: Set[DescriptorId], coeff: Map[Int, Double] = Map.empty) = {
    /**
     * Rows - props, cols - objects
     */
    val similar = storage.filter(kv => start(kv._1)).values.coalesce(1).reduce(_ + _)

    val multiplyVector = {
      val vect = DenseVector.ones[Double](cols)
      coeff.foreach(r => vect.update(r._1, r._2))

      vect
    }

    val vect = similar * multiplyVector

    vect.iterator
      .map { case (index, value) => new IdentifiableDouble(index, value) }
      .toSeq
      .sorted(Ordering[Identifiable[Double]].reverse)
  }

}
