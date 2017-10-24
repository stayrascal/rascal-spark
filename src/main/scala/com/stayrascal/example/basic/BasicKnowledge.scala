package com.stayrascal.example.basic

import com.twitter.util.{Promise, Try}

trait Resource {
  def imageLinks(): Seq[String]

  def links(): Seq[String]
}

class HTMLPage(val i: Seq[String], val l: Seq[String]) extends Resource {
  def imageLinks() = i

  def links = l
}

class Img() extends Resource {
  def imageLinks() = Seq()

  def links() = Seq()
}

object BasicKnowledge {
  val profile = new HTMLPage(Seq("portrait.jpg"), Seq("gallery.html"))
  val portrait = new Img

  val gallery = new HTMLPage(Seq("kitten.jpg", "puppy.jpg"), Seq("profile.html"))
  val kitten = new Img
  val puppy = new Img

  val internet = Map(
    "profile.html" -> profile,
    "gallery.html" -> gallery,
    "portrait.jpg" -> portrait,
    "kitten.jpg" -> kitten,
    "puppy.jpg" -> puppy
  )

  def fetch(url: String) = {
    new Promise(Try(internet(url)))
  }
}
