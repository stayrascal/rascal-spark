package com.stayrascal.spark.oozie
package conversion

import jobs._
import dsl._

case class RefSet[A <: AnyRef](vals: Seq[A]) extends Set[A] {
  override def contains(elem: A): Boolean = vals exists(e => e eq elem)

  def iterator: Iterator[A] = vals.iterator

  def -(elem: A): RefSet[A] = {
    if (!(this contains( elem ))){
      this
    } else {
      RefSet(vals filter ( _ ne elem))
    }
  }

  def +(elem: A): RefSet[A] = {
    if (this contains( elem )){
      this
    } else {
      RefSet(vals :+ elem)
    }
  }

  def ++(elems: RefSet[A]): RefSet[A] = (this /: elems)(_ + _)

  def --(elems: RefSet[A]): RefSet[A] = (this /: elems)(_ - _)

  def map[B <: AnyRef](f: (A) => B): RefSet[B] = {
    (RefSet[B]() /: vals) ((e1: RefSet[B], e2: A) => e1 + f(e2))
  }

  override def equals(that: Any): Boolean = {
    that match {
      case RefSet(otherVals) => this.vals.toSet.equals(otherVals.toSet)
      case _ => false
    }
  }
}

object RefSet {
  def apply[A <: AnyRef](): RefSet[A] = {
    RefSet(Seq.empty)
  }
  def apply[A <: AnyRef](elem: A, elems: A*): RefSet[A] = {
    RefSet(elem +: elems)
  }
}

case class RefWrap[T <: AnyRef](value: T) {
  override def equals(obj: scala.Any): Boolean = obj match {
    case ref: RefWrap[_] => ref.value eq value
    case _ => false
  }
}

case class RefMap[A <: AnyRef, B](vals: Map[RefWrap[A], B]) extends Map[RefWrap[A], B] {

  def +[B1 >: B](kv: (RefWrap[A], B1)) = {
    RefWrap(vals + kv)
  }

  def +[B1 >: B](kv: => (A, B1)): RefWrap[A, B1] = {
    val newKv = RefWrap(kv._1) -> kv._2
    RefWrap(vals + newKv)
  }

  def ++(rmap: RefMap[A, B]) = {
    (this /: rmap) (_ + _)
  }

  def -(key: RefWrap[A]) = {
    RefMap(vals - key)
  }

  def get(key: RefWrap[A]) = {
    vals get key
  }

  def get(key: => A): Option[B] = {
    vals get RefWrap(key)
  }

  def iterator: Iterator[(RefWrap[A], B)] = vals.iterator
}

object Flatten {
  def apply(workflow: WorkFlow): RefWrap[Dependency, GraphNode] = {
    var accum: RefMap[Dependency, GraphNode] = RefMap[Dependency, GraphNode](Map.empty)

    def fatten0(currentDep: Dependency, after: Set[GraphNode], inDecision: Boolean = false): Unit = {
      accum get currentDep match {
        case Some(alreadThere) =>
          currentDep match {
            case DecisionNode(decision, dependencies) => alreadThere.d
          }
      }
    }
  }
}
