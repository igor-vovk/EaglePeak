package com.igorvovk.eaglepeak.math.comparators

import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.rdd.RDD

case class ComparatorResult(matrix: IndexedRowMatrix)

trait Comparator[T] {

  def compare(objects: RDD[T]): ComparatorResult

}
