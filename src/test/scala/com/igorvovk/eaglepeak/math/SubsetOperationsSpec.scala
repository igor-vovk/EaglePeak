package com.igorvovk.eaglepeak.math

import org.specs2.mutable.Specification

class SubsetOperationsSpec extends Specification {

  import SubsetOperations._

  val store = List(
    Set("a", "b", "c"),
    Set("a", "c"),
    Set("b", "d"),
    Set("d")
  )

  "Find largest subset" >> {
    largestSubset(store, Set("a", "b", "c", "d")) should_== Some(store.head)
    largestSubset(store, Set("b", "c", "d")) should_== Some(Set("b", "d"))
    largestSubset(store, Set("d")) should_== Some(Set("d"))
    largestSubset(store, Set("f")) should_== None
  }

  "Find largest subsets" >> {
    largestSubsets(store, Set("a", "b", "c", "d")).reverse should_== Set("a", "b", "c") :: Set("d") :: Nil
  }

}
