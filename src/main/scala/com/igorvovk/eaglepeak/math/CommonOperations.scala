package com.igorvovk.eaglepeak.math

import breeze.linalg.{BitVector, CSCMatrix, Matrix}
import breeze.util.Index
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag

object CommonOperations {

  def index[T: ClassTag](df: DataFrame, column: String): Index[T] = {
    index(df.map(_.getAs[T](column)))
  }

  def index[T](rdd: RDD[T]): Index[T] = Index(rdd.distinct().toLocalIterator)

  def groupBy[K: ClassTag, V: ClassTag](df: DataFrame, keyCol: String, valueCol: String): (RDD[(K, Set[Int])], Index[V]) = {
    val valueIndex = index[V](df, valueCol)

    val grouped = mkPair[K, V](df, keyCol, valueCol).mapValues(valueIndex).groupByKey().mapValues(_.toSet)

    (grouped, valueIndex)
  }

  def extractDiscreteProps[K: ClassTag, V: ClassTag](df: DataFrame, keyCol: String, valueCol: String): (RDD[(K, BitVector)], Index[V]) = {
    val propertyIndex = index[V](df, valueCol)
    val propsCount = propertyIndex.size

    val grouped = mkPair[K, V](df, keyCol, valueCol).groupByKey().mapValues { case props =>
      BitVector(propsCount)(props.map(propertyIndex).toSeq: _*)
    }

    (grouped, propertyIndex)
  }

  def mkPair[K: ClassTag, V: ClassTag](df: DataFrame, keyCol: String, valueCol: String): RDD[(K, V)] = {
    df.map(row => row.getAs[K](keyCol) -> row.getAs[V](valueCol))
  }

  def rotateAndTranspose(algoMatrices: Seq[CoordinateMatrix]): RDD[(Int, Matrix[Double])] = {
    require(algoMatrices.nonEmpty)

    val simSize = algoMatrices.size
    val objCount = algoMatrices.head.numRows()
    val sc = algoMatrices.head.entries.sparkContext

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
      algoMatrix.entries.map(e => e.i.toInt -> e.copy(i = e.j, j = algoIndex))
    }

    val matricesByObjectIds = sc.union(entriesByObjectIds)
      .aggregateByKey(CSCMatrix.zeros[Double](objCount.toInt, simSize))(
        (m, e) => {
          m.update(e.i.toInt, e.j.toInt, e.value)
          m
        },
        _ + _
      )
      .mapValues(optimizeMatrix)

    matricesByObjectIds
  }

  private def optimizeMatrix(m: CSCMatrix[Double]): Matrix[Double] = {
    if (m.activeSize.toDouble / (m.rows * m.cols) > 0.75d) {
      m.toDense
    } else {
      CSCMatrix.Builder.fromMatrix(m).result
    }
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
