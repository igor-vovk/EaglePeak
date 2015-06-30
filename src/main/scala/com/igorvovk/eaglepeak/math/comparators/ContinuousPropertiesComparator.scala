package com.igorvovk.eaglepeak.math.comparators

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD

class ContinuousPropertiesComparator extends Comparator[Double] {

  override def compare(objects: RDD[Double]): ComparatorResult = {
    objects.cache()
    val stats = objects.stats()

    val normalized = objects.map(value => value - stats.min)
    val diff = stats.max - stats.min

    val indexed = normalized.zipWithIndex()
    indexed.cache()

    val size = indexed.count()

    val entries = indexed.cartesian(indexed).map { case ((a, i), (b, j)) =>
      val weight = math.abs(a - b) / diff

      MatrixEntry(i, j, weight)
    }

    val matrix = new CoordinateMatrix(entries, size, size)

    objects.unpersist(false)
    indexed.unpersist(false)

    ComparatorResult(matrix)
  }

}
