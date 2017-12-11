package com.stayrascal.spark.example.fp
package data.structure

sealed trait List[+A]

case object Nil extends List[Nothing]

case class Cons[+A](head: A, tail: List[A]) extends List[A]


object DataStructure {
  def tail[A](as: List[A]): List[A] = as match {
    case Nil => Nil
    case Cons(_, t) => t
  }


  def setHead[A](head: A, as: List[A]): List[A] = as match {
    case Nil => Cons(head, Nil)
    case Cons(_, t) => Cons(head, t)
  }

  def drop[A](l: List[A], n: Int): List[A] = {
    /*def go(n: Int): List[A] = {
      if (n > 1) tail(l)
      else go(n - 1)
    }
    go(n)*/

    if (n <= 0) l
    else l match {
      case Nil => Nil
      case Cons(_, t) => drop(l, n - 1)
    }
  }

  def dropWhile[A](l: List[A], f: A => Boolean): List[A] = {
    l match {
      case Cons(h, t) if f(h) => dropWhile(t, f)
      case _ => l
    }
  }

  def init[A](l: List[A]): List[A] = {
    l match {
      case Cons(_, Nil) => Nil
      case Cons(h, t) => Cons(h, init(t))
      case Nil => Nil
    }
  }

  def foldRight[A, B](as: List[A], z: B)(f: (A, B) => B): B = {
    as match {
      case Nil => z
      case Cons(x, xs) => f(x, foldRight(xs, z)(f))
    }
  }

  @annotation.tailrec
  def foldLeft[A, B](as: List[A], z: B)(f: (B, A) => B): B = as match {
    case Nil => z
    case Cons(h, xs) => foldLeft(xs, f(z, h))(f)
  }

  def sum(ns: List[Int]) = foldLeft(ns, 0)(_ + _)

  def product(ns: List[Double]) = foldLeft(ns, 1.0)(_ * _)

  def length[A](as: List[A]): Int = {
    foldRight(as, 0)((_, acc) => acc + 1)
  }

  def reverse[A](l: List[A]): List[A] = foldLeft(l, Nil:List[A])((acc, h) => Cons(h, acc))

  def foldRightViaFoldLeft[A, B](l: List[A], z: B)(f: (A, B) => B) = foldLeft(reverse(l), z)((b, a) => f(a, b))

  def appendViaFoldRight[A](l: List[A], r: List[A]): List[A] = foldRight(l, r)((h, acc) => Cons(h, acc))

  def appendViaFoldRight[A](l: List[A], r: List[A]): List[A] = foldLeft(l, r)((acc, h) => Cons(h, acc))

  def append[A](a1: List[A], a2: List[A]): List[A] =
    a1 match {
      case Nil => a2
      case Cons(h, t) => Cons(h, append(t, a2))
    }

  def concat[A](l: List[List[A]]): List[A] = foldLeft(l, Nil:List[A])(append)

  def add1[Int](l: List[Int]): List[Int] = foldRight(l, Nil:List[Int])((h, t) => Cons(h + 1, t))
}
