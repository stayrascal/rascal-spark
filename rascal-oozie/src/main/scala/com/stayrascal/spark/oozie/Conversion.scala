package com.stayrascal.spark.oozie

import java.util.Objects

import jobs._
import dsl._
import conversion._

sealed trait WorkflowOption

case class WorkflowJob(job: Job) extends WorkflowOption

case class WorkflowDecision(predicates: List[(String, dsl.Predicate)], decisionNode: DecisionNode) extends WorkflowOption

case object WorkflowFork extends WorkflowOption

case object WorkflowJoin extends WorkflowOption

case object WorkflowEnd extends WorkflowOption

object Conversion {
  val JobTracker = "${jobTracker}"
  val NameNode = "${nameNode}"

  def apply(workflow: WorkFlow): WORKFLOWu45APP = {
//    val flattenedNodes =
  }
}

case class GraphNode(var name: String,
                     var workflowOption: WorkflowOption,
                     val before: RefSet[GraphNode],
                     val after: RefSet[GraphNode],
                     val decisionBefore: RefSet[GraphNode],
                     val decisionAfter: RefSet[GraphNode],
                     var decisionRoutes: Set[(String, DecisionNode)] = Set.empty,
                     var errorTo: Option[GraphNode] = None) {
  def getName(n: GraphNode) = n.name

  def beforeNames = before.map(getName(_))

  def afterNames = after.map(getName(_))

  def decisionBeforeNames = decisionBefore.map(getName(_))

  def decisionAfterNames = decisionAfter.map(getName(_))

  override def toString = s"GraphNode: name=[$name], option=[$workflowOption], before=[$beforeNames], after=[$afterNames], decisionBefore=[$decisionBeforeNames], decisionAfter=[$decisionAfterNames], decisionRoute=[$decisionRoutes], errorTo=[$errorTo]"

  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case node: GraphNode =>
        this.name == node.name &&
        this.workflowOption == node.workflowOption &&
        this.beforeNames == node.beforeNames &&
        this.afterNames == node.afterNames &&
        this.decisionBeforeNames == node.decisionBeforeNames &&
        this.decisionAfterNames == node.decisionAfterNames &&
        this.decisionRoutes == node.decisionRoutes &&
        this.errorTo == node.errorTo
      case _ => false
    }
  }

  override def hashCode(): Int = {
    Objects.hashCode(name, workflowOption, beforeNames, afterNames, decisionBeforeNames, decisionAfterNames, decisionRoutes, errorTo)
  }
}
