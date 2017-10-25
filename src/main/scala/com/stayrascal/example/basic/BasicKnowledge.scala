package com.stayrascal.example.basic

import com.twitter.util.{Future, Promise, Try}

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

  def getThumbnail(url: String): Future[Resource] = {
    val returnVal = new Promise[Resource]
    fetch(url) flatMap { page =>
      fetch(page.imageLinks()(0)) onSuccess { p =>
        returnVal.setValue(p)
      } onFailure { exc =>
        returnVal.setException(exc)
      }
    } onFailure { exc =>
      returnVal.setException(exc)
    }
    returnVal
  }

  def getThumbnails(url: String): Future[Seq[Resource]] = {
    fetch(url) flatMap { page =>
      Future.collect(
        page.imageLinks map { u => fetch(u) }
      )
    }
  }

  def crawl(url: String): Future[Seq[Resource]] =
    fetch(url) flatMap { page =>
      Future.collect(
        page.links map { u => crawl(u) }
      ) map { pps => pps.flatten }
    }

  def main(args: Array[String]): Unit = {
    crawl("profile.html")
  }
}
