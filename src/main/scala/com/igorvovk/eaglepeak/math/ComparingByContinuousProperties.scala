package com.igorvovk.eaglepeak.math

import com.igorvovk.eaglepeak.domain.Descriptor
import com.igorvovk.eaglepeak.domain.Descriptor._
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class ComparingByContinuousProperties[K: ClassTag] extends ComparingAlgo[K, Double] {

  override def compare(objects: RDD[(K, Double)]): ComparingAlgoResult[K] = {
    objects.cache()
    val stats = objects.values.stats()

    val normalized = objects.mapValues(value => value - stats.min)
    val diff = stats.max - stats.min

    val indexed = normalized.zipWithIndex()
    indexed.cache()

    val size = indexed.count()
    val descriptors = indexed.map { case ((key, _), i) => new Descriptor[K](i, key) }

    val entries = indexed.cartesian(indexed).map { case (((_, aVal), i), ((_, bVal), j)) =>
      val weight = math.abs(aVal - bVal) / diff

      MatrixEntry(i, j, weight)
    }

    val matrix = new CoordinateMatrix(entries, size, size)

    objects.unpersist(false)
    indexed.unpersist(false)

    ComparingAlgoResult(descriptors, matrix)
  }

}
