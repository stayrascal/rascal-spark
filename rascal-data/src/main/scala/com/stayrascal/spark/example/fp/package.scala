package com.stayrascal.spark.example

sealed trait Tree[+A]

case class Leaf[A](value: A) extends Tree[A]

case class Branch[A](left: Tree[A], right: Tree[A]) extends Tree[A]

object Tree {
  def size[A](tree: Tree[A]): Int = tree match {
    case Branch(left, right) => size(left) + size(right) + 1
    case Leaf(_) => 1
  }

  def maximum(tree: Tree[Int]): Int = tree match {
    case Branch(left, right) => maximum(left) max maximum(right)
    case Leaf(n) => n
  }

  def depth[A](tree: Tree[A]): Int = tree match {
    case Branch(left, right) => (depth(left) max depth(right)) + 1
    case Leaf(_) => 0
  }

  def map[A, B](tree: Tree[A])(f: A => B): Tree[B] = tree match {
    case Branch(left, right) => Branch(map(left)(f), map(right)(f))
    case Leaf(n) => Leaf(f(n))
  }

  def fold[A, B](tree: Tree[A])(f: A => B)(g: (B, B) => B): B = tree match {
    case Branch(left, right) => g(fold(left)(f)(g), fold(right)(f)(g))
    case Leaf(n) => f(n)
  }
}
