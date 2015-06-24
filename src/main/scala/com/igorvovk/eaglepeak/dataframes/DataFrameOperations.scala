package com.igorvovk.eaglepeak.dataframes

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object DataFrameOperations {

  def distinct[T](df: DataFrame, column: String): RDD[T] = {
    df.select(column).distinct.map(_.getAs[T](column))
  }

  def groupBy[K, V](df: DataFrame, keyCol: String, valueCol: String): RDD[(K, Iterable[V])] = {
    df.select(keyCol, valueCol).map(row => {
      row.getAs[K](keyCol) -> row.getAs[V](valueCol)
    }).groupByKey()
  }

}
