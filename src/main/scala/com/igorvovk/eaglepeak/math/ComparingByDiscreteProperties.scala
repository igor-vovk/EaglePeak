package com.igorvovk.eaglepeak.math

import com.igorvovk.eaglepeak.domain.Descriptor
import com.igorvovk.eaglepeak.domain.Descriptor._
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD

class ComparingByDiscreteProperties[K] extends ComparingAlgo[K, Set[DescriptorId]] {

  val resultIfSame = 1d

  def compare(objects: RDD[(K, Set[DescriptorId])]): ComparingAlgoResult[K] = {
    val indexed = objects.zipWithIndex()
    indexed.cache()

    val size = indexed.count()
    val descriptors = indexed.map { case ((key, _), i) => new Descriptor[K](i, key) }

    val entries = indexed.cartesian(indexed).flatMap { case (((_, aProps), i), ((_, bProps), j)) =>
      val similar = Similarity.jaccard(aProps, bProps)
      if (similar > 0) {
        Iterator.single(MatrixEntry(i, j, similar))
      } else {
        Iterator.empty
      }
    }

    val matrix = new CoordinateMatrix(entries, size, size)

    indexed.unpersist(false)

    ComparingAlgoResult(descriptors, matrix)
  }

}
