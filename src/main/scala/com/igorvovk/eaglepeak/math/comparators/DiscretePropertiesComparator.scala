package com.igorvovk.eaglepeak.math.comparators

import com.igorvovk.eaglepeak.math.Similarity
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD

class DiscretePropertiesComparator[T] extends Comparator[Set[T]] {

  def compare(objects: RDD[(Set[T])]): ComparatorResult = {
    val indexed = objects.zipWithIndex()
    indexed.cache()

    val size = indexed.count()

    val entries = indexed.cartesian(indexed).flatMap { case ((a, i), (b, j)) =>
      val similar = Similarity.jaccard(a, b)
      if (similar > 0) {
        Iterator.single(MatrixEntry(i, j, similar))
      } else {
        Iterator.empty
      }
    }

    val matrix = new CoordinateMatrix(entries, size, size)

    indexed.unpersist(false)

    ComparatorResult(matrix)
  }

}
