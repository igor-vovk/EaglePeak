package com.igorvovk.eaglepeak.math.comparators

import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.rdd.RDD

case class ComparatorResult(matrix: CoordinateMatrix)

trait Comparator[T] {

  def compare(objects: RDD[T]): ComparatorResult

}
