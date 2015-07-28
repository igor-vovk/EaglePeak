package com.igorvovk.eaglepeak.math

import breeze.linalg.{BitVector, CSCMatrix, Matrix}
import breeze.util.Index
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.TraversableOnce
import scala.reflect.ClassTag

object CommonOperations {

  def mkIndex[T: ClassTag](df: DataFrame, column: String): Index[T] = {
    mkIndex(df.map(_.getAs[T](column)))
  }

  def mkIndex[T](rdd: RDD[T]): Index[T] = Index(rdd.distinct().toLocalIterator)

  def groupBy[K: ClassTag, V: ClassTag](df: DataFrame, keyCol: String, valueCol: String): (RDD[(K, Set[Int])], Index[V]) = {
    val valueIndex = mkIndex[V](df, valueCol)

    val grouped = mkPair[K, V](df, keyCol, valueCol).mapValues(valueIndex).groupByKey().mapValues(_.toSet)

    (grouped, valueIndex)
  }

  /**
   * Extract discrete props in non-grouped table, when keys are duplicating and each row contains one property
   *
   * Input:
   * |-----|-------|
   * | Key | Prop  |
   * |-----|-------|
   * | A   | Green |
   * | A   | Blue  |
   * | B   | Green |
   * | B   | White |
   * | B   | Red   |
   * |-----|-------|
   *
   * Output:
   *  1. RDD
   *     A -> [0, 1]
   *     B -> [0, 2, 3]
   *  2. Index
   *     0 <-> Green
   *     1 <-> Blue
   *     2 <-> White
   *     3 <-> Red
   */
  def extractDiscretePropsGrouping[K: ClassTag, V: ClassTag](df: DataFrame, keyCol: String, propCol: String): (RDD[(K, BitVector)], Index[V]) = {
    val propertyIndex = mkIndex[V](df, propCol)
    val propsCount = propertyIndex.size

    val grouped = mkPair[K, V](df, keyCol, propCol).groupByKey().mapValues(props => {
      BitVector(propsCount)(props.map(propertyIndex).toSeq: _*)
    })

    (grouped, propertyIndex)
  }

  /**
   * Same as above, but assumes that keys are already unique, and each row contains multiple properties in column,
   * like delim-separated properties
   */
  def extractDiscreteProps[K: ClassTag, V: ClassTag](df: DataFrame,
                                                     keyCol: String, propCol: String,
                                                     valueExtractor: String => TraversableOnce[V]): (RDD[(K, BitVector)], Index[V]) = {
    val propertyIndex = mkIndex(df.flatMap(row => valueExtractor(row.getAs[String](propCol))))
    val propsCount = propertyIndex.size

    val res = mkPair[K, String](df, keyCol, propCol).distinct().mapValues(props => {
      BitVector(propsCount)(valueExtractor(props).map(propertyIndex).toSeq: _*)
    })

    (res, propertyIndex)
  }

  def mkPair[K: ClassTag, V: ClassTag](df: DataFrame, keyCol: String, valueCol: String): RDD[(K, V)] = {
    df.map(row => row.getAs[K](keyCol) -> row.getAs[V](valueCol))
  }

  def rotateAndTranspose(algoMatrices: Seq[CoordinateMatrix]): RDD[(Int, Matrix[Double])] = {
    require(algoMatrices.nonEmpty)

    val simSize = algoMatrices.size
    val objCount = algoMatrices.head.numRows()

    val entriesByObjectIds = algoMatrices.zipWithIndex.map { case (m, algoIndex) =>
      val cols = m.numCols()
      val rows = m.numRows()

      require(
        cols == objCount && rows == objCount,
        s"All matrices should be square and have same dimensions " +
          s"(matrix #$algoIndex (RDD ${m.entries.name}} have dimensions R$rows x C$cols, must be $objCount)"
      )

      m.entries.map(e => e.i.toInt -> (e.j.toInt, algoIndex, e.value))
    }

    val matricesByObjectIds = union(entriesByObjectIds)
      .groupByKey()
      .mapValues(matrixFromCOO(objCount.toInt, simSize, _))

    matricesByObjectIds
  }

  /**
   * Generate a `SparseMatrix` from Coordinate List (COO) format. Input must be an array of
   * (i, j, value) tuples.
   */
  def matrixFromCOO(r: Int, c: Int, values: TraversableOnce[(Int, Int, Double)]): Matrix[Double] = {
    val sizeHint = if (values.hasDefiniteSize) values.size else 16

    val mb = new CSCMatrix.Builder[Double](r, c, sizeHint)
    values.foreach((mb.add _).tupled)

    mb.result
  }

  def union[T: ClassTag](seq: Seq[RDD[T]]): RDD[T] = {
    require(seq.nonEmpty)

    if (seq.size == 1) seq.head else seq.head.sparkContext.union(seq)
  }

  def optimizeMatrix(m: CSCMatrix[Double]): Matrix[Double] = {
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
