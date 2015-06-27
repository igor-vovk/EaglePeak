package com.igorvovk.eaglepeak.math

import com.igorvovk.eaglepeak.domain.Descriptor
import com.igorvovk.eaglepeak.domain.Descriptor._
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.SparkBreezeConverters._
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
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

  def rotateAndTranspose[AD](similarities: Seq[RowMatrix]): (Map[Long, IndexedRowMatrix]) = {
    if (similarities.nonEmpty) {
      val simSize = similarities.size
      val objCount = similarities.head.numRows()
      require(
        similarities.forall(m => m.numCols() == objCount && m.numRows() == objCount),
        "All matrices should be square and have same dimensions"
      )

      val indexedRddRows = similarities.map(_.rows.zipWithIndex().map { case (v, index) => new IndexedRow(index, v) })

      val rotated = (0L to objCount).map(objectIndex => {
        val similaritiesByAlgo = indexedRddRows.zipWithIndex.map { case (indexedRDD, algoId) =>
          indexedRDD.filter(_.index == objectIndex).map(row => row.copy(index = algoId))
        }.reduce(_ ++ _).sortBy(_.index)

        objectIndex -> new IndexedRowMatrix(similaritiesByAlgo, objCount, simSize)
      }).toMap

      rotated.mapValues(matrix => {
        matrix.toCoordinateMatrix().transpose().toIndexedRowMatrix()
      })
    } else {
      Map.empty
    }
  }

  /**
   *
   * @param storage Map, in which keys are identifiers of objects to start, and values are matrices with similarities.
   *                Rows are described as different objects, and columns as different properties
   * @param start Find similarities for this objects
   * @param coeff Coefficients applied to properties in matrix (rows in storage), default coefficient is 1
   * @return
   */
  def similar(storage: Map[Long, RowMatrix])(start: Set[Long], coeff: Map[Int, Double] = Map.empty) = {
    require(storage.nonEmpty)

    /**
     * Rows - props, cols - objects
     */
    val similar = addMatrices(start.map(storage).toSeq: _*)

    val multiplyMatrix = {
      val multiplyArray = Array.fill(similar.numCols())(1d)
      coeff.foreach(r => multiplyArray(r._1) = r._2)

      Matrices.dense(similar.numCols(), 1, multiplyArray)
    }

    similar.multiply(multiplyMatrix).rows.zipWithIndex().sortBy(_._1(0), ascending = false)
  }

  def addMatrices(matrices: IndexedRowMatrix*): IndexedRowMatrix = {
    require(matrices.nonEmpty, "Pass at least one matrix")

    if (matrices.length > 1) {
      val numRows = matrices.head.numRows()
      val numCols = matrices.head.numCols()
      require(matrices.forall(m => m.numRows() == numRows && m.numCols() == numCols), "Matrix must have same dimensions")

      val newRows = matrices.map(_.rows.map(ir => ir.index -> ir.vector.toBreeze)).reduce((a, b) => {
        a.zip(b).map { case ((ai, aRow), (bi, bRow)) =>
          require(ai == bi)

          ai -> (aRow + bRow)
        }
      }).map { case (index, vector) => IndexedRow(index, vector.toSpark) }

      new IndexedRowMatrix(newRows, numRows, numCols)
    } else {
      matrices.head
    }
  }

  def addMatrices(matrices: RowMatrix*): RowMatrix = {
    require(matrices.nonEmpty, "Pass at least one matrix")

    if (matrices.length > 1) {
      val numRows = matrices.head.numRows()
      val numCols = matrices.head.numCols()
      require(matrices.forall(m => m.numRows() == numRows && m.numCols() == numCols), "Matrix must have same dimensions")

      val newRows = matrices.map(_.rows.map(_.toBreeze)).reduce((aRows, bRows) => {
        aRows.zip(bRows).map(vectorsPair => vectorsPair._1 + vectorsPair._2)
      }).map(_.toSpark)

      new RowMatrix(newRows, numRows, numCols)
    } else {
      matrices.head
    }
  }

  /**
   * Union two maps a and b, executing mergeFunc on conflicting values
   */
  def unionWithKey[K, V](a: Map[K, V], b: Map[K, V], mergeFunc: (K, V, V) => V): Map[K, V] = {
    (a -- b.keySet) ++ b.map { case (k, v) =>
      k -> a.get(k).map(mergeFunc(k, v, _)).getOrElse(v)
    }
  }

}
