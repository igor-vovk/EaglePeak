package com.igorvovk.eaglepeak

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.specs2.specification.BeforeAfterAll

trait SparkBeforeAfter extends BeforeAfterAll {

  var sc: SparkContext = _
  var sqlc: SQLContext = _

  override def beforeAll(): Unit = {
    sc = new SparkContext("local", "test")
    sqlc = new SQLContext(sc)
  }

  override def afterAll(): Unit = {
    sc.stop()
    sqlc = null
    sc = null
  }

}
