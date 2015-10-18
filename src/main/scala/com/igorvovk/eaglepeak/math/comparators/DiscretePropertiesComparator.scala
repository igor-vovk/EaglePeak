package com.igorvovk.eaglepeak.math.comparators

import breeze.linalg.BitVector
import com.igorvovk.eaglepeak.math.Similarity
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class DiscretePropertiesComparator(minSimilarityLimit: Double = 0) extends Comparator[BitVector] {

  def compare(objects: RDD[BitVector]): ComparatorResult = {
    val indexed = objects.zipWithIndex().groupByKey()
    indexed.persist(StorageLevel.MEMORY_AND_DISK)

    val size = indexed.count()

    val limit = minSimilarityLimit
    val entries = indexed.cartesian(indexed).flatMap { case ((a, is), (b, js)) =>
      lazy val similar = Similarity.jaccard(a, b)

      for {
        i <- is
        j <- js
        if i <= j && similar > limit
        me <- if (i < j) {
          MatrixEntry(i, j, similar) :: MatrixEntry(j, i, similar) :: Nil
        } else {
          MatrixEntry(i, i, similar) :: Nil
        }
      } yield me
    }

    val matrix = new CoordinateMatrix(entries, size, size)

    ComparatorResult(matrix.toIndexedRowMatrix())
  }

}
