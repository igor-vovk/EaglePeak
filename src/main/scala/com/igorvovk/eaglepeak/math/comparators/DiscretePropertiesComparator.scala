package com.igorvovk.eaglepeak.math.comparators

import breeze.linalg.BitVector
import com.igorvovk.eaglepeak.math.Similarity
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD

class DiscretePropertiesComparator extends Comparator[BitVector] {

  def compare(objects: RDD[BitVector]): ComparatorResult = {
    val indexed = objects.zipWithIndex()
    indexed.cache()

    val size = indexed.count()

    val entries = indexed.cartesian(indexed).flatMap { case ((a, i), (b, j)) =>
      if (i <= j) {
        val similar = Similarity.jaccard(a, b)
        if (similar > 0) {
          Iterator(MatrixEntry(i, j, similar), MatrixEntry(j, i, similar))
        } else {
          Iterator.empty
        }
      } else {
        Iterator.empty
      }
    }

    val matrix = new CoordinateMatrix(entries, size, size)

    indexed.unpersist(false)

    ComparatorResult(matrix)
  }

}
