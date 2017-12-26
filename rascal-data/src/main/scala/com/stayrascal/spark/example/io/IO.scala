package com.stayrascal.spark.example.io

import com.stayrascal.spark.example.fp.monod.Monad

trait IO {
  self =>
  def run: Unit

  def ++(io: IO): IO = new IO {
    override def run: Unit = {
      self.run; io.run
    }
  }
}

object IO {
  def empty: IO = new IO {
    override def run: Unit = ()
  }
}

object IO1 {
  sealed trait IO[A] {
    self =>
    def run: A

    def map[B](f: A => B): IO[B] = new IO[B] {
      override def run = f(self.run)
    }

    def flatMap[B](f: A => IO[B]): IO[B] = new IO[B] {
      override def run = f(self.run).run
    }

  }

  object IO extends Monad[IO]{
    override def unit[A](a: => A): IO[A] = new IO[A] {
      override def run = a
    }

    override def flatMap[A, B](fa: IO[A])(f: A => IO[B]) = fa flatMap f

    def apply[A](a: => A): IO[A] = unit(a)
  }
}
