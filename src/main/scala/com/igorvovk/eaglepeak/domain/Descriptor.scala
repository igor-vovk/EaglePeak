package com.igorvovk.eaglepeak.domain

import com.igorvovk.eaglepeak.domain.Descriptor.DescriptorId

object Descriptor {
  type DescriptorId = Int

  implicit def longToDescriptorId(long: Long): DescriptorId = long.toInt
}

/**
 * Descriptor having unique id and description
 */
class Descriptor[+T](val id: DescriptorId, val description: T) extends Serializable {

  def tuple: (Int, T) = (id, description)

  def tupleInv: (T, Int) = (description, id)

  override def equals(obj: Any) = obj match {
    case that: Descriptor[T] => this.id == that.id
    case _ => false
  }

  override def hashCode() = id.hashCode()

  override def toString: String = s"$id -> $description"
}
