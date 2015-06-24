package com.igorvovk.eaglepeak.domain

/**
 * Descriptor having unique id and description
 * @TODO maybe id should be [[Long]]
 */
class Descriptor[+T](val id: Int, val description: T) {

  override def equals(obj: Any) = obj match {
    case that: Descriptor[T] => this.id == that.id
    case _ => false
  }

  override def hashCode() = id
}
