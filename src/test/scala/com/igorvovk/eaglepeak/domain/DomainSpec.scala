package com.igorvovk.eaglepeak.domain

import org.specs2.mutable.Specification

class DomainSpec extends Specification {

  "Descriptor" >> {

    "equality checks" >> {
      val a = new Descriptor(1, "A")
      val b = new Descriptor(1, "B")
      val c = new Descriptor(2, "A")

      a should_== b
      a should_!= c
    }

  }

}
