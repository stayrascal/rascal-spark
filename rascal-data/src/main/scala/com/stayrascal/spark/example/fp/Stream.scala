package com.stayrascal.spark.example.fp
package stream

sealed trait Stream[+A] {
  def toListRecursive: List[A] = this match {
    case Cons(h, t) => h() :: t().toListRecursive
    case _ => List()
  }

  def toList: List[A] = {
    @annotation.tailrec
    def go(s: Stream[A], acc: List[A]): List[A] = s match {
      case Cons(h, t) => go(t(), h() :: acc)
      case _ => acc
    }

    go(this, List()).reverse
  }

  import Stream._

  def take(n: Int): Stream[A] = this match {
    case Cons(h, t) if n > 1 => cons(h(), t().take(n-1))
    case Cons(h, _) if n == 1 => cons(h(), Empty)
    case _ => Empty
  }

  def takeWhile(p: A => Boolean): Stream[A] = this match {
    case Cons(h, t) if p(h()) => cons(h(), t().take(n - 1))
    case _ => Empty
  }

  def takeWhileViaFoldRight(p: A => Boolean): Stream[A] = {
    foldRight(Empty[A])((a, b) => if(p(a)) cons(a, b) else Empty)
  }

  def headOption: Option[A] = foldRight(None: Option[A])((h, _) => Some(h))


  def drop(n: Int): Stream[A] = this match {
    case Cons(_, t) if n > 0 => t().drop(n -1)
    case _ => this
  }

  def foldRight[B](z: => B)(f: (A, => B) => B): B = this match {
    case Cons(h, t) => f(h(), t().foldRight(z)(f))
    case _ => z
  }

  def forAll(p: A => Boolean): Boolean = {
    foldRight(true)((a, b) => p(a) && b)
  }

  def map[B](f: A => B): Stream[B] = foldRight(Empty[B])((a, b) => cons(f(a), b))

  def filter(f : A => Boolean): Stream[A] = foldRight(Empty[A])((a, b) => if (f(a)) cons(a, b) else b )

  def append[B >: A](s: => Stream[B]): Stream[B] = foldRight(s)((h, t) => cons(h, t))

  def flatMap[B](f: A => Stream[B]): Stream[B] = foldRight(Empty[B])((h, t) => f(h) append t)


}

case object Empty extends Stream[Nothing]

case class Cons[+A](h: () => A, t: () => Stream[A]) extends Stream[A]


object Stream {

  def cons[A](h: => A, t: => Stream[A]) : Stream[A] = {
    lazy val hh = h
    lazy val tt = t
    Cons(() => hh, () => tt)
  }

  def constant[A](a: A): Stream[A] = {
    lazy val tail: Stream[A] = cons(a, tail)
    tail
  }

  def from(n: Int): Stream[Int] = cons(n, from(n + 1))

  def fibs = {
    def go(pre: Int, curr: Int): Stream[Int] = {
      cons(curr, go(curr, curr + pre))
    }
    go(0, 1)
  }

  def unfold[A, S](z: S)(f: S => Option[(A, S)]): Stream[A] = {
    f(z) match {
      case Some((h, s)) => cons(h, unfold(s)(f))
      case _ => Empty
    }
  }



}
