package com.igorvovk.eaglepeak.math

import scala.annotation.tailrec

/**
 * WARNING! Storage must be sorted by [[SubsetOperations.storageOrdering]]!
 */
object SubsetOperations {

  val storageOrdering = Ordering.by[Set[_], Int](_.size).reverse

  @tailrec
  private def largestSubset[T](in: List[Set[T]], of: Set[T], setSize: Int): (Set[T], List[Set[T]]) = {
    in match {
      case Nil => (null, List.empty)
      case s :: xs =>
        if (s.subsetOf(of)) {
          (s, xs)
        } else {
          largestSubset(xs, of, setSize)
        }
    }
  }

  @tailrec
  private def largestSubsets[T](storage: List[Set[T]], set: Set[T], mem: List[Set[T]]): List[Set[T]] = {
    storage match {
      case Nil => mem
      case _ =>
        largestSubset(storage, set, set.size) match {
          case (null, Nil) => mem
          case (sub, leftStorage) => largestSubsets(leftStorage, set -- sub, sub :: mem)
        }
    }
  }

  def largestSubset[T](storage: List[Set[T]], set: Set[T]): Option[Set[T]] = {
    Option(largestSubset(storage, set, set.size)._1)
  }

  def largestSubsets[T](storage: List[Set[T]], set: Set[T]): List[Set[T]] = {
    largestSubsets(storage, set, List.empty).reverse
  }

}
