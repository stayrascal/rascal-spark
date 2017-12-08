package com.stayrascal.spark.oozie

import java.io.ByteArrayInputStream

import scala.xml.NodeSeq

case class WorkFlow(name: String, actions: List[OozieAction])

case class OozieAction(nameNode: String, jobTracker: String, prepare: OoziePrepare, settingFile: String, properties: List[OozieProperty],
                       master: String, mode: String, jobName: String, mainClass: String, dependenciesJar: String, sparkOpts: String,
                       args: List[String], ok: String, error: String)

case class OoziePrepare(path: String)

case class OozieProperty(name: String, value: String)

object OozieWorkflow {

  import scala.util.parsing.json.JSON._

  def toJsonObject(xmlObject: NodeSeq): List[WorkFlow] = {

  }

  def createWorkFlow(json: ByteArrayInputStream): String = {
    val config = new JsonXMLConfigBuilder()
  }

  val toXml(json: String): xml = {
    val workFlow: WorkFlow = json match {
      case Some(workflow: WorkFlow) => workFlow
    }
    val xmlOut =
      <workflow-app name={workFlow.name} xmlns="uri:oozie:workflow:0.5">
        {workFlow.actions.foreach(action =>
        <action name={action.nameNode}>
          <spark xmlns="uri:oozie:spark-action:0.1">
            <job-tracker>{action.jobTracker}</job-tracker>
            <name-node>{action.nameNode}</name-node>
            <prepare>
              <delete path={action.prepare.path}/>
              <mkdir path={action.prepare.path}/>
            </prepare>
            <job-xml>{action.settingFile}</job-xml>
            <configuration>
              {action.properties.foreach { property =>
              <property>
                <name>{property.name}</name>
                <value>{property.value}</value>
              </property>
            }}
            </configuration>
            <master>{action.master}</master>
            <mode>{action.mode}</mode>
            <name>{action.jobName}</name>
            <class>{action.mainClass}</class>
            <jar>{action.dependenciesJar}</jar>
            <spark-opts>{action.sparkOpts}</spark-opts>
            {action.args.map(arg => <arg>{arg}</arg>)}
          </spark>
          <ok to={action.ok}/>
          <error to={action.error}/>
        </action>
      )}
      </workflow-app>
  }

  def parse(data: String) = {
    val content = parseFull(data) match {
      case Some(m: Map[String, _]) =>
        <workflow-app name={m.get("flowName")} xmlns="uri:oozie:workflow:0.5">
          {m.get("actions") match {
          case Some(action: Map[String, _]) =>
            <action name={action.get("name")}>
              <spark xmlns="uri:oozie:spark-action:0.1">
                <job-tracker>
                  {action.get("jobTracker")}
                </job-tracker>
                <name-node>
                  {action.get("name")}
                </name-node>
                <job-xml>
                  {action.get("sparkSettingFile")}
                </job-xml>
                <master>
                  {action.get("masterUrl")}
                </master>
                <mode>
                  {action.get("sparkMode")}
                </mode>
                <name>
                  {action.get("jobName")}
                </name>
                <class>
                  {action.get("mainClass")}
                </class>
                <jar>
                  {action.get("dependent")}
                </jar>
                <spark-opts>
                  {action.get("sparkOptions")}
                </spark-opts>
              </spark>
            </action>.toString()
        }}
        </workflow-app>.toString()
    }
    println(content)
  }

  def main(args: Array[String]): Unit = {
    parse("""{"flowName":"flowName", actions:[{"name":"actionName", "jobTracker": "localhost:8021", "sparkSettingFile": "filePath", "masterUrl": "url"}]}""")
  }
}
