package com.igorvovk.eaglepeak.dataframes

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag

object DataFrameOperations {

  def distinct[T: ClassTag](df: DataFrame, column: String): RDD[T] = {
    df.select(column).distinct.map(_.getAs[T](column))
  }

  def groupBy[K: ClassTag, V: ClassTag](df: DataFrame, keyCol: String, valueCol: String): RDD[(K, Iterable[V])] = {
    df.select(keyCol, valueCol).map(row => {
      row.getAs[K](keyCol) -> row.getAs[V](valueCol)
    }).groupByKey()
  }

}
