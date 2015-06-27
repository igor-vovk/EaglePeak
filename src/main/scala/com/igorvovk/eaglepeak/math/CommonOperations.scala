package com.igorvovk.eaglepeak.math

import com.igorvovk.eaglepeak.domain.Descriptor
import com.igorvovk.eaglepeak.domain.Descriptor._
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag

object CommonOperations {

  def buildDescriptors[T: ClassTag](df: DataFrame, column: String): RDD[Descriptor[T]] = {
    buildDescriptors(df.map(_.getAs[T](column)))
  }

  def buildDescriptors[T](rdd: RDD[T]): RDD[Descriptor[T]] = {
    rdd.distinct().zipWithIndex().map { case (description, id) => new Descriptor(id, description) }
  }

  def groupBy[K: ClassTag, V: ClassTag](df: DataFrame, keyCol: String, valueCol: String): (RDD[(K, Set[DescriptorId])], Array[Descriptor[V]]) = {
    val valueDescriptors = buildDescriptors[V](df, valueCol).collect()
    val descriptionToId = valueDescriptors.map(_.tupleInv).toMap

    val grouped = mkPair[K, V](df, keyCol, valueCol).mapValues(descriptionToId).groupByKey().mapValues(_.toSet)

    (grouped, valueDescriptors)
  }

  def mkPair[K: ClassTag, V: ClassTag](df: DataFrame, keyCol: String, valueCol: String): RDD[(K, V)] = {
    df.map(row => row.getAs[K](keyCol) -> row.getAs[V](valueCol))
  }

  def transposeMatrices[T](matrices: Map[T, IndexedRowMatrix]): (Map[Long, IndexedRowMatrix], Seq[Descriptor[T]]) = {
    val descriptors = matrices.keys.zipWithIndex.map { case (descr, i) => new Descriptor[T](i, descr) }
    val descriptorsByDescr = descriptors.map(_.tupleInv).toMap

    val objectsCount = matrices.head._2.numCols()
    val transposed = (0L to objectsCount).map(i => {
      i -> new IndexedRowMatrix(matrices.map { case (description, matrix) =>
        matrix.rows.filter(_.index == i).map(row => row.copy(index = descriptorsByDescr(description)))
      }.reduce(_ ++ _))
    }).toMap

    //    val transposed = matrices.flatMap { case (descr, matrix) =>
    //      matrix.rows.map {
    //        case IndexedRow(index, vector) => index -> (descriptorsByDescr(descr), vector)
    //      }.collect()
    //    }.groupByKey().mapValues(iter => {
    //      new IndexedRowMatrix(sc.parallelize(iter.map(f => new IndexedRow(f._1, f._2)).toSeq))
    //    })

    (transposed, descriptors.toSeq)
  }

  def similar(matrix: IndexedRowMatrix) = {
    val matrixNumRows = matrix.numRows().toInt

    matrix.toCoordinateMatrix().transpose().toIndexedRowMatrix().rows.aggregate(Map.empty[Long, Array[Double]])(
      (mem: Map[Long, Array[Double]], row: IndexedRow) => {
        mem.getOrElse(row.index, Array.fill(matrixNumRows)(0d))
        row.vector match {
          case SparseVector(length, keys, values) =>
          case DenseVector(values) =>
        }
        mem
      },
      (a, b) => a ++ b
    )
  }

}
