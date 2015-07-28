package com.igorvovk.eaglepeak.examples

import org.apache.spark.sql.{DataFrame, SQLContext}

object Fixtures {

  protected def resource(name: String) = getClass.getClassLoader.getResource(name).getPath

  def athlettes(sqlc: SQLContext) = {
    sqlc.read
      .format("com.databricks.spark.csv").options(Map("header" -> "true", "delimiter" -> ";"))
      .load(resource("OlympicAthletes.csv"))
  }

  case class MovieLens(movies: DataFrame, ratings: DataFrame)

  def movielens(sqlc: SQLContext, dir: String) = {
    val reader = sqlc.read
      .format("com.databricks.spark.csv").options(Map("header" -> "true", "delimiter" -> ","))

    MovieLens(
      reader.load(resource(dir + "/movies.csv")),
      reader.load(resource(dir + "/ratings.csv"))
    )
  }

}
