package com.stayrascal.spark

import java.io.{FileInputStream, PrintWriter}
import java.time.LocalDate
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import com.stayrascal.spark.pipeline.{EmployeePicker, RascalCombiner, StringIndexerParam}
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.sql.SparkSession
import org.eclipse.jetty.server.Request
import org.eclipse.jetty.server.handler.AbstractHandler
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

import scala.pickling.Defaults._
import scala.pickling.binary.{BinaryPickle, _}

class RascalHandler(spark: SparkSession, combiner: RascalCombiner) extends AbstractHandler {

  implicit val formats = DefaultFormats

  private def handleSparkJob(employeeIds: List[String], date: String, combiner: RascalCombiner): String = {
    val stream = new FileInputStream("combiner.txt")
    val pickle = BinaryPickle(stream)
    val params = pickle.unpickle[Map[String, StringIndexerParam]]
    println(params.mapValues(p => p.uid).mkString(","))
    val indexers = params.mapValues(p => new StringIndexerModel(p.uid, p.labels).setInputCol(p.inputCol).setOutputCol(p.outputCol))
    combiner.stringIndexers = indexers
    val picker = new EmployeePicker(combiner, "/data", spark)
    val opList = picker.pickIds(employeeIds, LocalDate.parse(date))
    write(opList)
  }

  override def handle(context: String, request: Request, httpServletRequest: HttpServletRequest, httpServletResponse: HttpServletResponse): Unit = {
    val employeeIds = httpServletRequest.getParameter("employee_id").split(",").toList
    val date = if (httpServletRequest.getParameter("date") == null) "2017-01-01" else httpServletRequest.getParameter("date")

    var resp = ""
    try {
      resp = handleSparkJob(employeeIds, date, combiner)
    } catch {
      case e: Exception => {
        resp = write(Map("msg" -> e.getMessage))
      }
    }

    httpServletResponse.setContentType("application/json; charset=utf-8")
    httpServletResponse.setStatus(HttpServletResponse.SC_OK)
    val out: PrintWriter = httpServletResponse.getWriter
    out.println(resp)
    request.setHandled(true)
  }


}
