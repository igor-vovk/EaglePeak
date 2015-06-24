package com.igorvovk.eaglepeak.math

import com.igorvovk.eaglepeak.domain.Descriptor
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD

class ComparingByDiscreteProperties[K, V] extends ComparingAlgo[K, Iterable[V]] {

  val resultIfSame = 1d

  def compare(objects: RDD[(K, Iterable[V])]): ComparingAlgoResult[K] = {
    val indexed = objects.zipWithIndex()
    indexed.persist()

    val descriptors = indexed.map { case ((key, _), i) => new Descriptor[K](i.toInt, key) }

    val matrix = new CoordinateMatrix(indexed.cartesian(indexed).map { case (((aKey, aProps), ai), ((bKey, bProps), bi)) =>
      val compareResult =
        if (aKey == bKey) resultIfSame
        else {
          val bSet = bProps.toSet

          1d / aProps.filterNot(bSet).size
        }

      MatrixEntry(ai, bi, compareResult)
    })

    indexed.unpersist()

    ComparingAlgoResult(descriptors, matrix.toRowMatrix())
  }

}
