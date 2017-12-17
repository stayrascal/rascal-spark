package com.stayrascal.spark.example.fp

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
    case Cons(h, t) if n > 1 => cons(h(), t().take(n - 1))
    case Cons(h, _) if n == 1 => cons(h(), Empty)
    case _ => Empty
  }

  def takeWhile(p: A => Boolean): Stream[A] = this match {
    case Cons(h, t) if p(h()) => cons(h(), t().takeWhile(p))
    case _ => Empty
  }

  def takeWhileViaFoldRight(p: A => Boolean): Stream[A] = {
    foldRight(Empty[A])((a, b) => if (p(a)) cons(a, b) else Empty)
  }

  def headOption: Option[A] = foldRight(None: Option[A])((h, _) => Some(h))


  def drop(n: Int): Stream[A] = this match {
    case Cons(_, t) if n > 0 => t().drop(n - 1)
    case _ => this
  }

  def foldRight[B](z: => B)(f: (A, => B) => B): B = this match {
    case Cons(h, t) => f(h(), t().foldRight(z)(f))
    case _ => z
  }

  def exists(p: A => Boolean): Boolean = foldRight(false)((a, b) => p(a) && b)

  def forAll(p: A => Boolean): Boolean = {
    foldRight(true)((a, b) => p(a) && b)
  }

  def map[B](f: A => B): Stream[B] = foldRight(Empty[B])((a, b) => cons(f(a), b))

  def mapViaUnfold[B](f: A => B): Stream[B] = unfold(this) {
    case Cons(h, t) => Some((f(h()), t()))
    case _ => None
  }

  def takeViaUnfold(n: Int): Stream[A] = unfold(this, n) {
    case (Cons(h, t), 1) => Some((h(), (Empty, 0)))
    case (Cons(h, t), n) if n > 1 => Some((h(), (t(), n - 1)))
    case _ => None
  }

  def takeWhileViaFold(p: A => Boolean): Stream[A] = unfold(this) {
    case Cons(h, t) if p(h()) => Some((h(), t()))
    case _ => None
  }

  def zipWith[B, C](s2: Stream[B])(f: (A, B) => C): Stream[C] = unfold((this, s2)) {
    case (Cons(h1, t1), Cons(h2, t2)) => Some(f(h1(), h2()), (t1(), t2()))
    case _ => None
  }

  def zipWithAll[B, C](s2: Stream[B])(f: (Option[A], Option[B]) => C): Stream[C] = unfold((this, s2)) {
    case (Empty, Empty) => None
    case (Cons(h, t), Empty) => Some(f(Some(h()), Option.empty[B]) -> (t(), Empty[B]))
    case (Empty, Cons(h, t)) => Some(f(Option.empty[A], Some(h())) -> (Empty[A], t()))
    case (Cons(h1, t1), Cons(h2, t2)) => Some(f(Some(h1()), Some(h2())) -> (t1() -> t2()))
  }

  def zipAll[B](s2: Stream[B]): Stream[(Option[A], Option[B])] = zipWithAll(s2)((_, _))

  def startsWith[A](s: Stream[A]): Boolean = zipAll(s).takeWhile(!_._2.isEmpty) forAll {
    case (h, h2) => h == h2
  }

  def tails: Stream[Stream[A]] = unfold(this) {
    case Empty => None
    case s => Some((s, s drop 1))
  } append Empty

  def hasSubsequence[A](s: Stream[A]): Boolean = tails exists( _ startsWith s)

  def scanRight[B](z : B)(f: (A, => B) => B): Stream[B] = foldRight((x, Stream(z)))((a, p0) => {
    lazy val p1 = p0
    val b2 = f(a, p1._1)
    (b2, cons(b2, p1._2))
  })._2

  def filter(f: A => Boolean): Stream[A] = foldRight(Empty[A])((a, b) => if (f(a)) cons(a, b) else b)

  def append[B >: A](s: => Stream[B]): Stream[B] = foldRight(s)((h, t) => cons(h, t))

  def flatMap[B](f: A => Stream[B]): Stream[B] = foldRight(Empty[B])((h, t) => f(h) append t)


}

case object Empty extends Stream[Nothing]

case class Cons[+A](h: () => A, t: () => Stream[A]) extends Stream[A]


object Stream {

  def cons[A](h: => A, t: => Stream[A]): Stream[A] = {
    lazy val hh = h
    lazy val tt = t
    Cons(() => hh, () => tt)
  }

  def empty[A]: Stream[A] = Empty

  def apply[A](as: A*): Stream[A] = {
    if (as.isEmpty) empty
    else cons(as.head, apply(as.tail: _*))
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

  def unfoldViaFold[A, S](z: S)(f: S => Option[(A, S)]): Stream[A] = {
    f(z).fold(Empty[A])((p: (A, S)) => cons(p._1, unfoldViaFold(p._2)(f)))
  }

  def unfoldViaMap[A, S](z: S)(f: S => Option[(A, S)]): Stream[A] = {
    f(z).map((p: (A, S)) => cons(p._1, unfoldViaMap(p._2)(f))).getOrElse(Empty[A])
  }

  def fibsViaUnfold = {
    unfold((0, 1)) { case (pre, cur) => Some((pre, (cur, pre + cur))) }
  }

  def fromViaUnfold(n: Int): Stream[Int] = unfold(n)(n => Some((n, n + 1)))

  def constantViaUnfold[A](a: A): Stream[A] = unfold(a)(a => Some((a, a)))

  def onesViaUnfold() = constantViaUnfold(1)
}
