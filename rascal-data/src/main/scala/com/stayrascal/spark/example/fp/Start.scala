package com.stayrascal.spark.example.fp

object Start {
  def fib(n: Int): Int = {

    def go(n: Int): Int = {
      if (n <= 1) n
      else go(n - 1) + go(n - 2)
    }

    @annotation.tailrec
    def loop(n: Int, pre: Int, cur: Int): Int = {
      if (n == 0) pre
      else loop(n - 1, cur, pre + cur)
    }

//    go(n)
    loop(n, 0, 1)
  }

  def isSorted[A](as: Array[A], ordered: (A, A) => Boolean): Boolean = {
    @annotation.tailrec
    def go(n: Int): Boolean = {
      if (n >= as.length - 1) true
      else if (!ordered(as(n), as(n+1))) false
      else go(n + 1)
    }
    go(0)
  }

  def main(args: Array[String]): Unit = {
    println(fib(6))
  }


}
