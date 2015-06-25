package com.igorvovk.eaglepeak.dataframes

import com.igorvovk.eaglepeak.SparkBeforeAfter
import org.specs2.mutable.Specification

class DataFrameOperationsSpec extends Specification with SparkBeforeAfter {

  import DataFrameOperations._

  val tuples = Seq[(String, Int)](("a", 1), ("b", 2), ("c", 3), ("a", 4))

  "buildDescriptors" >> {
    val df = sqlc.createDataFrame(tuples)
    val descriptors = buildDescriptors[String](df, "_1").collect()

    descriptors must have size 3
  }

  "groupBy" >> {
    val df = sqlc.createDataFrame(tuples)
    val (groupedRDD, _) = groupBy[String, Int](df, "_1", "_2")
    val grouped = groupedRDD.collect().toMap

    grouped must have size 3
    grouped must haveKey("a")
    grouped("a") must have size 2
  }

}
