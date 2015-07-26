package com.igorvovk.eaglepeak.math.recommendations

import java.util

import breeze.collection.mutable.Beam
import breeze.linalg
import breeze.linalg.{DenseMatrix, DenseVector, Matrix}
import com.igorvovk.eaglepeak.domain.{Identifiable, IdentifiableDouble}
import com.twitter.chill.ResourcePool

/**
 * First implementation of merging and sorting algorithm. Did not use any caches, but support scoring.
 * Good for small amounts of input data.
 *
 *
 * @param storage Map, in which keys are identifiers of objects to start, and values are matrices with similarities.
 *                Rows are described as different objects, and columns as different properties
 */
class RawFirst(storage: Int => Option[Matrix[Double]], rows: Int, cols: Int) {

  val tempArrPool = new ResourcePool[Array[Double]](Runtime.getRuntime.availableProcessors) {
    override def newInstance() = {
      val a = new Array[Double](rows * cols)

      util.Arrays.fill(a, 0, a.length, 0.0d)

      a
    }

    override def release(item: Array[Double]): Unit = {
      util.Arrays.fill(item, 0, item.length, 0.0d)

      super.release(item)
    }
  }

  /**
   *
   * @param start Find similarities for this objects
   * @param coeff Coefficients applied to properties in matrix (rows in storage), default coefficient is 1
   * @return
   */
  def similar(start: linalg.Vector[Double], coeff: Map[Int, Double] = Map.empty, limit: Int): Iterable[Identifiable[Double]] = {
    var temp: Matrix[Double] = DenseMatrix.create(rows, cols, tempArrPool.borrow())

    for (itemId <- start.activeKeysIterator) {
      val itemO = storage(itemId)
      if (itemO.isDefined) {
        val item = itemO.get

        val itemWeight = start(itemId)
        temp += (if (itemWeight == 1.0d) item else item * itemWeight)
      }
    }

    /**
     * Rows - props, cols - objects
     */
    val multiplyVector = DenseVector.tabulate[Double](cols)(coeff.getOrElse(_, 1.0d))

    val vec = temp * multiplyVector
    tempArrPool.release(temp.asInstanceOf[DenseMatrix[Double]].data)

    start.activeKeysIterator.foreach(i => vec.update(i, 0d)) // Exclude starting points

    val beam = Beam[Identifiable[Double]](limit)

    beam ++= vec.activeIterator
      .map { case (index, value) => new IdentifiableDouble(index, value) }

    beam
  }

}
