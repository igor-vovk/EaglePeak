package com.igorvovk.eaglepeak

import org.apache.spark.sql.SQLContext

object Fixtures {

  protected val athletesFilePath = getClass.getClassLoader.getResource("OlympicAthletes.csv").getPath

  def athlettes(sqlc: SQLContext) = {
    sqlc.read
      .format("com.databricks.spark.csv").options(Map("header" -> "true", "delimiter" -> ";"))
      .load(athletesFilePath)
  }

}
