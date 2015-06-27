package com.igorvovk.eaglepeak.math

import com.igorvovk.eaglepeak.domain.Descriptor
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

case class ComparingAlgoResult[K](descriptors: RDD[Descriptor[K]], matrix: RowMatrix)

trait ComparingAlgo[K, T] {

  def compare(objects: RDD[(K, T)]): ComparingAlgoResult[K]

}
