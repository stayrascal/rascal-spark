package com.stayrascal.spark.example.fp.parallelism

import java.util.concurrent._


object Par {
  type Par[A] = ExecutorService => Future[A]

  def run[A](s: ExecutorService)(a: Par[A]): Future[A] = a(s)

  def unit[A](a: A): Par[A] = (es: ExecutorService) = UnitFuture(a)

  def lazyUnit[A](a: A): Par[A] = fork(unit(a))

  private case class UnitFuture[A](get: A) extends Future[A] {
    def isDone = true

    def get(timeout: Long, units: TimeUnit) = get

    def isCancelled: Boolean = false

    def cancel(evenIfRunning: Boolean): Boolean = false
  }

  def map2[A, B, C](a: Par[A], b: Par[B])(f: (A, B) => C): Par[C] = (es: ExecutorService) => {
    val af = a(es)
    val bf = b(es)
    UnitFuture(f(af.get, bf.get))
  }

  def fork[A](a: => Par[A]): Par[A] = es => es.submit(new Callable[A] {
    def call = a(es).get
  })

  def asyncF[A, B](f: A => B): A => Par[B] = a => lazyUnit(f(a))

  def sortPar(parList: Par[List[Int]]): Par[List[Int]] = map2(parList, unit(()))((a, _) => a.sorted)

  def map[A, B](pa: Par[A])(f: A => B): Par[B] = map2(pa, unit(()))((a, _) => f(a))

  def sequence_simple[A](ps: List[Par[A]]): Par[List[A]] = ps.foldRight[Par[List[A]]](unit(List()))((h, t) => map2(h, t)(_ :: _))

  def sequenceRight[A](as: List[Par[A]]): Par[List[A]] = as match {
    case Nil => unit(Nil)
    case h :: t => map2(h, fork(sequenceRight(t)))(_ :: _)
  }

  def sequenceBalanced[A](as: IndexedSeq[Par[A]]): Par[IndexedSeq[A]] = fork {
    if (as.isEmpty) unit(Vector())
    else if (as.length == 1) map(as.head)(a => Vector(a))
    else {
      val (l, r) = as.splitAt(as.length / 2)
      map2(sequenceBalanced(l), sequenceBalanced(r))(_ ++ _)
    }
  }

  def sequence[A](ps: List[Par[A]]): Par[List[A]] = map(sequenceBalanced(ps.toIndexedSeq))(_.toList)

  def parMap[A, B](ps: List[A])(f: A => B): Par[List[B]] = fork {
    val fbs: List[Par[B]] = ps.map(asyncF(f))
    sequence(fbs)
  }

  def parFilter[A](as: List[A])(f: A => Boolean): Par[List[A]] = {
    val pars: List[Par[List[A]]] = as map(asyncF((a: A) => if (f(a)) List(a) else List()))
    map(sequence(pars))(_.flatten)
  }
}
