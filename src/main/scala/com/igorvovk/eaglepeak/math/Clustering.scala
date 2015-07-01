package com.igorvovk.eaglepeak.math

import org.apache.spark.rdd.RDD

object Clustering {

  type ScoringFunc = (Set[_], Int) => Int

  val default: ScoringFunc = (set, count) => set.size * (count - 1)

  /**
   * Detect subsets in [in] sets, sorted descending by [scoring] function
   * Note: also filter elements, for which [scoring] function return score <= 0
   *
   * @return RDD with subsets and their scores
   */
  def detectClusters[T](in: RDD[Set[T]], scoring: ScoringFunc = default): RDD[(Set[T], Int)] = {
    val setsWithScore = in.flatMap(_.subsets().map(_ -> 1)).reduceByKey(_ + _).map { case (set, count) =>
      set -> scoring(set, count)
    }

    setsWithScore.filter(_._2 > 0).sortBy(_._2, ascending = false)
  }

}
