package com.stayrascal.example.game24

trait BinaryOp {
  val op: String

  def apply(expr1: String, expr2: String) = expr1 + op + expr2

  def unapply(str: String): Option[(String, String)] = {
    val index = str.indexOf(op)
    if (index > 0) Some(str.substring(0, index), str.substring(index + 1)) else None
  }
}

object Multiply extends {
  val op = "*"
} with BinaryOp

object Divide extends {
  val op = "/"
} with BinaryOp

object Add extends {
  val op = "+"
} with BinaryOp

object Subtract extends {
  val op = "-"
} with BinaryOp