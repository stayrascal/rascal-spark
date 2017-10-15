package com.stayrascal.example.game24

class Rational(n: Int, d: Int) {
  require(d != 0)
  private val g = gcd(n.abs, d.abs)
  val number = n / g
  val denominator = d / g

  override def toString: String = number + "\\" + denominator

  def +(that: Rational): Rational = new Rational(number * that.denominator + that.number * denominator, denominator * that.denominator)

  def +(i: Int): Rational = new Rational(number + denominator, denominator)

  def -(that: Rational): Rational = new Rational(number * that.denominator - that.number * denominator, denominator * that.denominator)

  def *(that: Rational): Rational = new Rational(number * that.number, denominator * that.denominator)

  def /(that: Rational): Rational = new Rational(number * that.denominator, that.number * denominator)

  def this(n: Int) = this(n, 1)

  private def gcd(a: Int, b: Int): Int = if (b == 0) a else gcd(b, a % b)
}

object Rational extends {
  val op = "\\"
} with BinaryOp