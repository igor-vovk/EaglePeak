package com.igorvovk.eaglepeak.math

import breeze.linalg.{CSCMatrix, Matrix}
import com.igorvovk.eaglepeak.domain.Descriptor
import com.igorvovk.eaglepeak.domain.Descriptor._
import org.apache.spark.mllib.linalg.distributed._
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

  def rotateAndTranspose(algoMatrices: Seq[CoordinateMatrix]): RDD[(Int, Matrix[Double])] = {
    require(algoMatrices.nonEmpty)

    val simSize = algoMatrices.size
    val objCount = algoMatrices.head.numRows()

    algoMatrices.zipWithIndex.foreach { case (m, i) =>
      val cols = m.numCols()
      val rows = m.numRows()

      require(
        cols == objCount && rows == objCount,
        s"All matrices should be square and have same dimensions " +
          s"(matrix #$i (RDD ${m.entries.name}} have dimensions R$rows x C$cols, must be $objCount)"
      )
    }

    val entriesByObjectIds = algoMatrices.zipWithIndex.map { case (algoMatrix, algoIndex) =>
      algoMatrix.entries.map { case MatrixEntry(row, col, v) =>
        row.toInt -> MatrixEntry(col, algoIndex, v)
      }
    }.reduce(_ ++ _).groupByKey()

    val matricesByObjectIds = entriesByObjectIds.mapValues(mkMatrixFromEntries(_, objCount, simSize))

    matricesByObjectIds
  }

  def mkMatrixFromEntries(entries: Iterable[MatrixEntry], rows: Int, cols: Int): Matrix[Double] = {
    val builder = new CSCMatrix.Builder[Double](rows, cols)
    entries.foreach(entry => builder.add(entry.i.toInt, entry.j.toInt, entry.value))

    val m = builder.result

    if (m.activeSize.toDouble / (rows * cols) > 0.75d) m.toDense else m
  }

  def filterSelfIndices(indice: Int, similarities: Matrix[Double]) = {
    (0 to similarities.cols).foreach(col => {
      similarities.update(indice, col, 0d)
    })
  }

}
