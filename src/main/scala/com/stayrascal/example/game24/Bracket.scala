package com.stayrascal.example.game24

import scala.collection.mutable.Stack

object Bracket {
  def matchBracket(str: String): Option[(Int, Int)] = {
    val left = str.indexOf('(')
    if (left >= 0) {
      val stack = Stack[Char]()
      val remaining = str.substring(left + 1)
      var index = 0
      var right = 0
      for (c <- remaining if right == 0) {
        index = index + 1
        c match {
          case '(' => stack.push(c)
          case ')' => if (stack isEmpty) right = left + index else stack.pop()
          case _ =>
        }
      }
      Some(left, right)
    } else None
  }

  def apply(part1: String, expr: String, part2: String) = part1 + "(" + expr + ")" + part2

  def unapply(str: String): Option[(String, String, String)] = {
    Bracket.matchBracket(str) match {
      case Some((left: Int, right: Int)) => {
        val part1 = if (left == 0) "" else str.substring(0, left)
        val expr = str.substring(left + 1, right)
        val part2 = if (right == str.length - 1) "" else str.substring(right + 1)
        Some(part1, expr, part2)
      }
      case _ => None
    }
  }
}
