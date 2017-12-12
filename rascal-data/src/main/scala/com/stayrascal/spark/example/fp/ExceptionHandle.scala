package com.stayrascal.spark.example.fp
package error.handle

import scala.{Option => _, Either => _}


sealed trait Option[+A] {
  def map[B] (f: A => B): Option[B] = this match {
    case None => None
    case Some(a) => Some(f(a))
  }

  def flatMap[B] (f: A => Option[B]): Option[B] = map(f) getOrElse None

  def flatMap_1[B] (f: A => Option[B]): Option[B] = this match {
    case None => None
    case Some(a) => f(a)
  }

  def getOrElse[B >: A](default: => B):B = this match {
    case None => default
    case Some(a) => a
  }

  def orElse[B >: A](obj: => Option[B]): Option[B] = this match {
    case None => obj
    case _ => this
  }

  def orElse_1[B >: A](obj: => Option[B]): Option[B] = this map (Some(_)) getOrElse obj

  def filter(f: A=> Boolean): Option[A] = this match {
    case Some(a) if(f(a)) => Some(a)
    case _ => None
  }

  def filter_1(f: A=> Boolean): Option[A] = flatMap(a => if (f(a)) Some(a) else  None)
}

case class Some[+A](get: A) extends Option[A]

case object None extends Option[Nothing]

object Option {

}

object ExceptionHandle {

  def mean(xs: Seq[Double]): Option[Double] = {
    if (xs.isEmpty) None
    else Some(xs.sum / xs.length)
  }

  def variance(xs: Seq[Double]): Option[Double] = {
    mean(xs) flatMap (m => mean(xs.map(x => math.pow(x - m, 2))))
  }

  def map2[A, B, C](a: Option[A], b: Option[B])(f: (A, B) => C): Option[C] = (a, b) match {
    case (None, None) => None
    case (a, b) => Some(f(a, b))
  }

  def map2_1[A, B, C](a: Option[A], b: Option[B])(f: (A, B) => C): Option[C] = a flatMap (aa => b map (bb => f(aa, bb)))

  def sequence[A](a: List[Option[A]]): Option[List[A]] = {
    a match {
      case Nil => Some(Nil)
      case h :: t => h flatMap (hh => sequence(t) map (hh :: _))
    }
  }

  def sequence_1[A](a: List[Option[A]]): Option[List[A]] = {
    a.foldRight[Option[List[A]]](Some(Nil))((x, y) => map2(x, y)(_ :: _))
  }

  def traverse[A, B](a: List[A])(f: A => Option[B]): Option[List[B]] = {
    a match {
      case Nil => Some(Nil)
      case h :: t => map2(f(h), traverse(t)(f)) (_ :: _)
    }
  }

  def traverse_1[A, B](a: List[A])(f: A => Option[B]): Option[List[B]] = {
    a.foldRight[Option[List[B]]](Some(Nil))((h, t) => map2(f(h), t)(_ :: _))
  }

  def sequenceViaTraverse[A](a: List[Option[A]]): Option[List[A]] = traverse(a)(x => x)
}

sealed trait Either[+E, +A] {
  def map[B](f: A => B): Either[E, B] = this match {
    case Left(e) => Left(e)
    case Right(a) => Right(f(a))
  }

  def flatMap[EE >: E, B](f: A => Either[EE, B]): Either[EE, B] = this match {
    case Left(e) => Left(e)
    case Right(a) => f(a)
  }

  def orElse[EE >: E, B >: A](b: => Either[EE, B]): Either[EE, B] = this match {
    case Left(e) => b
    case Right(a) => Right(a)
  }

  def map2[EE >: E, B, C](b: Either[EE, B])(f: (A, B) => C): Either[EE, C] = for {a <- this; b1 <- b} yield f(a, b1)
}

case class Left[+E](value: E) extends Either[E, Nothing]

case class Right[+A](value: A) extends Either[Nothing, A]

object Either {
 def traverse[E, A, B](es: List[A])(f: A => Either[E, B]): Either[E, List[B]] = es match {
   case Nil => Right(Nil)
   case h :: t => (f(h) map2 traverse(t)(f))(_ :: _)
 }

  def traverse_1[E, A, B](es: List[A])(f: A => Either[E, B]): Either[E, List[B]] = es.foldRight[Either[E, List[B]]](Right(Nil))((a, b) => f(a).map2(b)(_ :: _))

  def sequence[E, A](es: List[Either[E, A]]): Either[E, List[A]] = traverse(es)(x => x)
}
