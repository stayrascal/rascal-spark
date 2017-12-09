package com.stayrascal.spark.oozie
package jobs

import dsl._

sealed trait FsTask

case class MkDir(path: String) extends FsTask

case class Mv(from: String, to: String) extends FsTask

case class Rm(path: String) extends FsTask

case class Touchz(path: String) extends FsTask

case class ChMod(path: String, permissions: String, dirFiles: String) extends FsTask

case class JavaJob(mainClass: String, prepare: List[FsTask] = List.empty,
                   configuration: ArgList = Nil, jvmOps: Option[String] = None, args: List[String] = Nil) extends Job {
  val domain = mainClass.substring(mainClass.lastIndexOf(".") + 1)
  override val jobName = s"java_$domain"
}

case class NoOpJob(name: String) extends Job {
  override val jobName = name
}

case class MapReduceJob(name: String, prepare: List[FsTask] = List.empty, configuration: ArgList = Nil) extends Job {
  override val jobName = s"mr_$name"
}

case class FsJob(name: String, tasks: List[FsTask]) extends Job {
  override val jobName = s"fs_$name"
}

case class HiveJob(fileName: String,
                   configuration: ArgList = Nil,
                   parameters: List[String] = List.empty,
                   prepare: List[FsTask] = List.empty,
                   jobXml: Option[Seq[String]] = None,
                   otherFiles: Option[Seq[String]] = None) extends Job {
  val dotIndex = fileName.indexOf(".")
  val cleanName = {
    if (dotIndex > 0) {
      fileName.substring(0, dotIndex)
    } else {
      fileName
    }
  }
  override val jobName = s"hive_$cleanName"
}

object `package` {
  type ArgList = List[(String, String)]

  def verifySuccessPaths(paths: List[String]): List[String] = {
    val checkedPaths = paths map (currString => {
      val headString = {
        if (!currString.startsWith("${nameNode}")) {
          val newStr = {
            if (currString.startsWith("/")) {
              "${nameNode}"
            } else {
              "${nameNode}/"
            }
          }
          newStr
        } else ""
      }
      val tailString = {
        if (!currString.endsWith("_SUCCESS")) {
          val newStr = {
            if (currString.last != '/') {
              "/_SUCCESS"
            } else {
              "_SUCCESS"
            }
          }
          newStr
        } else ""
      }
      headString + currString + tailString
    })
    checkedPaths
  }
}