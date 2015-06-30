package com.igorvovk.eaglepeak.domain

import scala.collection.GenIterable

object Identifiable {

  implicit def ord[T](implicit o: Ordering[T]): Ordering[Identifiable[T]] = {
    Ordering.by[Identifiable[T], T](_.value)
  }

}

trait Identifiable[+T] extends Serializable {

  val id: Int

  val value: T

  override def equals(obj: scala.Any): Boolean = obj match {
    case that: IdentifiableDouble => this.value == that.value
    case _ => false
  }

  override def hashCode(): Int = value.hashCode()

  override def toString: String = s"$id -> $value"

}

class IdentifiableSeq[T, +A <: GenIterable[T]](val id: Int, val value: A) extends Identifiable[A]

class IdentifiableDouble(val id: Int, val value: Double) extends Identifiable[Double]