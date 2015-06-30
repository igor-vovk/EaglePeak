package com.igorvovk.eaglepeak.math

import com.igorvovk.eaglepeak.domain.Descriptor
import com.igorvovk.eaglepeak.domain.Descriptor._
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class ComparingByContinuousPropertiesZoned[K: ClassTag](zonesCount: Int = 5) extends ComparingAlgo[K, Double] with Serializable {

  override def compare(objects: RDD[(K, Double)]): ComparingAlgoResult[K] = {
    objects.cache()
    val stats = objects.values.stats()

    val step = (stats.max - stats.min) / zonesCount
    val steps = (stats.min to stats.max by step).map(min => (min, min + step)).zipWithIndex.toSeq
    val normalized = objects.mapValues(value => steps.find { case ((min, max), _) => min <= value && value <= max }.get._2)

    val indexed = normalized.zipWithIndex()
    indexed.cache()

    val size = indexed.count()
    val descriptors = indexed.map { case ((key, _), i) => new Descriptor[K](i, key) }

    val entries = indexed.cartesian(indexed).flatMap { case (((_, aVal), i), ((_, bVal), j)) =>
      val weight = (zonesCount - math.abs(aVal - bVal)) / zonesCount

      if (weight > 0) {
        Iterator.single(MatrixEntry(i, j, weight))
      } else {
        Iterator.empty
      }
    }

    val matrix = new CoordinateMatrix(entries, size, size)

    objects.unpersist(false)
    indexed.unpersist(false)

    ComparingAlgoResult(descriptors, matrix)
  }

}
