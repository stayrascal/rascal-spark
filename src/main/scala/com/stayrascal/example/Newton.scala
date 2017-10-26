package com.stayrascal.example

object Newton {

  def sqrt(x: Double): Double = {
    def sqrtIter(guess: Double): Double = {
      if (isGoodResult(guess)) {
        guess
      } else {
        sqrtIter((guess + x / guess) / 2)
      }
    }

    def isGoodResult(guess: Double): Boolean = abd(x - guess) < 0.001

    def abd(d: Double): Double = if (d > 0) d else -d

    sqrtIter(1)
  }
}
