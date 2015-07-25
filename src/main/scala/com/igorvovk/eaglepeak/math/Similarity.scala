package com.igorvovk.eaglepeak.math

import breeze.linalg.BitVector

object Similarity {

  /**
   * Sparse vector representation
   */
  type SparseVector[A] = Map[A, Double]

  /**
   * Implementation of cosine similarity
   * @return cosine similarity between a and b (normalized by Euclidean norm).
   */
  def cosine[T](a: SparseVector[T], b: SparseVector[T]): Double = {
    val dotProduct = a.keySet.intersect(b.keySet)
      .map(key => a(key) * b(key))
      .foldLeft(0d)(_ + _)

    val normA = math.sqrt(a.values.map(v => math.pow(v, 2d)).foldLeft(0d)(_ + _))
    val normB = math.sqrt(b.values.map(v => math.pow(v, 2d)).foldLeft(0d)(_ + _))

    dotProduct / (normA + normB)
  }

  /**
   * [[https://en.wikipedia.org/wiki/Jaccard_index]]
   * Also known as Tanimoto coefficient (dealing with bit vectors)
   * @return
   */
  def jaccard[T](a: BitVector, b: BitVector): Double = {
    val num = (a & b).activeSize
    if (num > 0) {
      val den = (a + b).activeSize.toDouble

      num.toDouble / den
    } else {
      0d
    }
  }

}
