package com.igorvovk.eaglepeak.math

import breeze.linalg.{BitVector, CSCMatrix, Matrix}
import breeze.util.Index
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.SparkBreezeConverters._
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

  def rotateAndTranspose(algoMatrices: Seq[IndexedRowMatrix]): RDD[(Int, Matrix[Double])] = {
    require(algoMatrices.nonEmpty)

    val simSize = algoMatrices.size

    val entriesByObjectIds = algoMatrices.zipWithIndex.map { case (m, algoId) =>
      m.rows.map(r => r.index.toInt -> r.copy(index = algoId))
    }

    val matricesByObjectIds = union(entriesByObjectIds)
      .groupByKey()
      .mapValues(rows => {
        var mb: CSCMatrix.Builder[Double] = null

        rows.foreach(row => {
          val index = row.index.toInt
          val v = row.vector.toBreeze

          if (null == mb) {
            mb = new CSCMatrix.Builder(v.size, simSize)
          }

          v.foreachPair(mb.add(_, index, _))
        })

        mb.result.asInstanceOf[Matrix[Double]]
      })

    matricesByObjectIds
  }

  /**
   * Generate a `SparseMatrix` from Coordinate List (COO) format. Input must be an array of
   * (i, j, value) tuples.
   */
  def matrixFromCOO(rows: Int, cols: Int, values: TraversableOnce[(Int, Int, Double)]): Matrix[Double] = {
    val sizeHint = if (values.hasDefiniteSize) values.size else 16

    val mb = new CSCMatrix.Builder[Double](rows, cols, sizeHint)
    values.foreach((mb.add _).tupled)

    mb.result
  }

  def union[T: ClassTag](seq: Seq[RDD[T]]): RDD[T] = {
    require(seq.nonEmpty)

    if (seq.size == 1) seq.head else seq.head.sparkContext.union(seq)
  }

  def optimizeMatrix(m: Matrix[Double]): Matrix[Double] = {
    m match {
      case a: DenseMatrix => a // Nothing to optimize
      case a: CSCMatrix[Double] =>
        if (a.activeSize.toDouble / a.size.toDouble > 0.75d) {
          a.toDense
        } else {
          CSCMatrix.Builder.fromMatrix(a).result
        }
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
