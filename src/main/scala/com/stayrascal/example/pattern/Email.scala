package com.stayrascal.example.pattern

object Email {

  def apply(user: String, domain: String) = user + "@" + domain

  def unapply(str: String): Option[(String, String)] = {
    val parts = str.split("@")
    if (parts.length == 1) Some(parts(0), parts(1)) else None
  }
}

object Twice {
  def apply(s: String) = s + s

  def unapply(s: String): Option[String] = {
    val length = s.length / 2
    val half = s.substring(0, length)
    if (half == s.substring(length)) Some(half) else None
  }
}

object UpperCase {
  def unapply(s: String): Boolean = s.toUpperCase == s
}

object Domain {
  def apply(parts: String*): String = parts.reverse.mkString(".")

  def unapplySeq(whole: String): Option[Seq[String]] = Some(whole.split("\\.").reverse)
}

object Main {
  def userTwiceUpper(s: String) = s match {
    case Email(Twice(x@UpperCase()), domain) => "match:" + x + " in domain " + domain
    case _ => "no match"
  }
}



