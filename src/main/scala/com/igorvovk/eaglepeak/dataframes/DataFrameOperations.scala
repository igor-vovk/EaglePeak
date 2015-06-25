package com.igorvovk.eaglepeak.dataframes

import com.igorvovk.eaglepeak.domain.Descriptor
import com.igorvovk.eaglepeak.domain.Descriptor._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag

object DataFrameOperations {

  def buildDescriptors[T: ClassTag](df: DataFrame, column: String): RDD[Descriptor[T]] = {
    df.map(_.getAs[T](column)).distinct().zipWithIndex().map {
      case (description, id) => new Descriptor(id, description)
    }
  }

  def groupBy[K: ClassTag, V: ClassTag](df: DataFrame, keyCol: String, valueCol: String): (RDD[(K, Set[DescriptorId])], Array[Descriptor[V]]) = {
    val kv = df.select(keyCol, valueCol)

    val valueDescriptors = buildDescriptors[V](kv, valueCol).collect()
    val descriptorsByDescription = valueDescriptors.map(_.tupleInv).toMap

    val grouped = kv.map(row => {
      row.getAs[K](keyCol) -> descriptorsByDescription(row.getAs[V](valueCol))
    }).groupByKey().mapValues(_.toSet)

    (grouped, valueDescriptors)
  }

}
