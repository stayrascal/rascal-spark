package com.stayrascal.spark.example.fp.monod

trait Functor[F[_]] {
  def map[A, B](fa: F[A])(f: A => B): F[B]

  def distribute[A, B](fab: F[(A, B)]): (F[A], F[B]) = (map(fab)(_._1), map(fab)(_._2))

  def codistribute[A, B](e: Either[F[A], F[B]]): F[Either[A, B]] = e match {
    case Left(fa) => map(fa)(Left(_))
    case Right(fb) => map(fb)(Right(_))
  }
}

trait Monad[F[_]] extends Functor[F] {
  def unit[A](a: => A): F[A]

  def flatMap[A, B](ma: F[A])(f: A => F[B]): F[B] = join(map(ma)(f))

  override def map[A, B](fa: F[A])(f: A => B): F[B] = flatMap(fa)(ma => unit(f(ma)))

  def map2[A, B, C](ma: F[A], mb: F[B])(f: (A, B) => C): F[C] = flatMap(ma)(a => map(mb)(b => f(a, b)))

  def sequence[A](lma: List[F[A]]): F[List[A]] = lma.foldLeft(unit(List[A]()))((acc, x) => map2(x, acc)(_ :: _))

  def traverse[A, B](la: List[A])(f: A => F[B]): F[List[B]] = la.foldLeft(unit(List[B]()))((acc, x) => map2(f(x), acc)(_ :: _))

  def _replicateM[A](n: Int, ma: F[A]): F[List[A]] = {
    if (n <= 0) unit(List()) else map2(ma, _replicateM(n - 1, ma)(_ :: _))
  }

  def replicateM[A](n: Int, ma: F[A]): F[List[A]] = {
    sequence(List.fill(n)(ma))
  }

  def product[A, B](ma: F[A], mb: F[B]): F[(A, B)] = map2(ma, mb)((_, _))

  def compose[A, B, C](f: A => F[B], g: B => F[C]): A => F[C] = x => flatMap(f(a))(g)

  def _flatMap[A, B](ma: F[A])(f: A => F[B]): F[B] = compose((_: Unit) => ma, f)(())

  def join[A](mma: F[F[A]]): F[A] = flatMap(mma)(x => x)

  def filterM[A](ms: List[A])(f: A => F[Boolean]): F[List[A]] =
    ms.foldRight(unit(List[A]()))((x, y) =>
      compose(f, (b: Boolean) => if (b) map2(unit(x), y)(_ :: _) else y)(x))

}

object Monad {
  val optionMonad = new Monad[Option] {
    override def unit[A](a: => A) = Some(a)

    override def flatMap[A, B](ma: Option[A])(f: A => Option[B]) = ma.flatMap(f)
  }

  val streamMonad = new Monad[Stream] {
    override def unit[A](a: => A) = Stream(a)

    override def flatMap[A, B](ma: Stream[A])(f: A => Stream[B]) = ma flatMap f
  }
}
