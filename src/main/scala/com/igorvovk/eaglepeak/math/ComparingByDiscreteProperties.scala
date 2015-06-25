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

    val descriptors = indexed.map { case ((key, _), i) => new Descriptor[K](i, key) }

    val matrix = new CoordinateMatrix(indexed.cartesian(indexed).map { case (((aKey, aProps), i), ((bKey, bProps), j)) =>
      val compareResult =
        if (aKey == bKey) resultIfSame
        else {
          aProps.count(bProps) / (aProps ++ bProps).size
        }

      MatrixEntry(i, j, compareResult)
    }).toIndexedRowMatrix()

    indexed.unpersist(false)

    ComparingAlgoResult(descriptors, matrix)
  }

}
