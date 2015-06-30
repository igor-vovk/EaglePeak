package com.igorvovk.eaglepeak.math.comparators

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD

class ContinuousPropertiesZonedComparator(zonesCount: Int = 5) extends Comparator[Double] {

  override def compare(objects: RDD[Double]): ComparatorResult = {
    objects.cache()
    val stats = objects.stats()

    val step = (stats.max - stats.min) / zonesCount
    val steps = (stats.min to stats.max by step).map(min => (min, min + step)).zipWithIndex.toSeq
    val normalized = objects.map(value => steps.find { case ((min, max), _) => min <= value && value <= max }.get._2)

    val indexed = normalized.zipWithIndex()
    indexed.cache()

    val size = indexed.count()

    val entries = indexed.cartesian(indexed).flatMap { case ((a, i), (b, j)) =>
      val weight = (zonesCount - math.abs(a - b)) / zonesCount

      if (weight > 0) {
        Iterator.single(MatrixEntry(i, j, weight))
      } else {
        Iterator.empty
      }
    }

    val matrix = new CoordinateMatrix(entries, size, size)

    objects.unpersist(false)
    indexed.unpersist(false)

    ComparatorResult(matrix)
  }

}
