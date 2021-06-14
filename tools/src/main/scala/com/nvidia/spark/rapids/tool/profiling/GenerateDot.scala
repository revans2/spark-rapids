/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nvidia.spark.rapids.tool.profiling

import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.ToolTextFileWriter

import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.metric.SQLMetricInfo

class DotGraph(
    val id: String,
    val label: String,
    val labelLoc: String = "b",
    val fontName: String = "Courier") {

  val nodes = new ArrayBuffer[DotNode]()
  val links = new ArrayBuffer[DotLink]()

  def addNode(node: DotNode): Unit = {
    nodes += node
  }

  def addLink(link: DotLink): Unit = {
    links += link
  }

  def write(writer: ToolTextFileWriter): Unit = {
    writer.write(
      s"""
         |digraph $id {
         |  label="$label"
         |  labelloc=$labelLoc
         |  fontname=$fontName
         |""".stripMargin)

    nodes.foreach(_.write(writer))
    links.foreach(_.write(writer))

    writer.write("}\n")
  }
}

class DotLink(
    val leftId: String,
    val rightId: String,
    val color: String,
    val style: String = "bold") {
  def write(writer: ToolTextFileWriter): Unit =
    writer.write(s"""$leftId -> $rightId [color="$color",style=$style];\n""")
}

class DotNode(
    val id: String,
    val label: String,
    val color: String,
    val shape: String = "box",
    val style: String = "filled") {

  def write(writer: ToolTextFileWriter): Unit =
    writer.write(s"""$id [shape=$shape,color="$color",style="$style",label="$label"];\n""")
}

/**
 * Generate a DOT graph for one query plan, or showing differences between two query plans.
 *
 * Diff mode is intended for comparing query plans that are expected to have the same
 * structure, such as two different runs of the same query but with different tuning options.
 *
 * When running in diff mode, any differences in SQL metrics are shown. Also, if the plan
 * starts to deviate then the graph will show where the plans deviate and will not recurse
 * further.
 *
 * Graphviz and other tools can be used to generate images from DOT files.
 *
 * See https://graphviz.org/pdf/dotguide.pdf for a description of DOT files.
 */
object GenerateDot {
  private val GPU_COLOR = "#76b900" // NVIDIA Green
  private val CPU_COLOR = "#0071c5"
  private val TRANSITION_COLOR = "red"

  /**
   * Generate a query plan visualization in dot format.
   *
   * @param plan First query plan and metrics
   * @param comparisonPlan Optional second query plan and metrics
   * @param sqlId id of the SQL query for the dot graph
   * @param appId Spark application Id
   */
  def writeDotGraph(
    plan: QueryPlanWithMetrics,
    physicalPlanString: String,
    comparisonPlan: Option[QueryPlanWithMetrics],
    fileWriter: ToolTextFileWriter,
    sqlId: Long,
    appId: String
  ): Unit = {

    var nextId = 1

    def isGpuPlan(plan: SparkPlanInfo): Boolean = {
      plan.nodeName match {
        case name if name contains "QueryStage" =>
          plan.children.isEmpty || isGpuPlan(plan.children.head)
        case name if name == "ReusedExchange" =>
          plan.children.isEmpty || isGpuPlan(plan.children.head)
        case name =>
          name.startsWith("Gpu")
      }
    }

    def formatMetric(m: SQLMetricInfo, value: Long): String = {
      val formatter = java.text.NumberFormat.getIntegerInstance
      m.metricType match {
        case "timing" =>
          val ms = value
          s"${formatter.format(ms)} ms"
        case "nsTiming" =>
          val ms = TimeUnit.NANOSECONDS.toMillis(value)
          s"${formatter.format(ms)} ms"
        case _ =>
          s"${formatter.format(value)}"
      }
    }

    /** Recursively graph the operator nodes in the spark plan */
    def buildGraph(
        graph: DotGraph,
        node: QueryPlanWithMetrics,
        comparisonNode: QueryPlanWithMetrics,
        id: Int = 0): Unit = {

      val nodePlan = node.plan
      val comparisonPlan = comparisonNode.plan
      if (nodePlan.nodeName == comparisonPlan.nodeName &&
        nodePlan.children.length == comparisonPlan.children.length) {

        val metricNames = (nodePlan.metrics.map(_.name) ++
          comparisonPlan.metrics.map(_.name)).distinct.sorted

        val metrics = metricNames.flatMap(name => {
          val l = nodePlan.metrics.find(_.name == name)
          val r = comparisonPlan.metrics.find(_.name == name)
          (l, r) match {
            case (Some(metric1), Some(metric2)) =>
              (node.metrics.get(metric1.accumulatorId),
                comparisonNode.metrics.get(metric1.accumulatorId)) match {
                case (Some(value1), Some(value2)) =>
                  if (value1 == value2) {
                    Some(s"$name: ${formatMetric(metric1, value1)}")
                  } else {
                    metric1.metricType match {
                      case "nsTiming" | "timing" =>
                        val pctStr = createPercentDiffString(value1, value2)
                        Some(s"$name: ${formatMetric(metric1, value1)} / " +
                          s"${formatMetric(metric2, value2)} ($pctStr %)")
                      case _ =>
                        Some(s"$name: ${formatMetric(metric1, value1)} / " +
                          s"${formatMetric(metric2, value2)}")
                    }
                  }
                case _ => None
              }
            case _ => None
          }
        }).mkString("\n")

        val color = if (isGpuPlan(nodePlan)) { GPU_COLOR } else { CPU_COLOR }

        val label = if (nodePlan.nodeName.contains("QueryStage")) {
          nodePlan.simpleString
        } else {
          nodePlan.nodeName
        }

        graph.addNode(new DotNode(s"node$id", s"$label\n$metrics", color))

        nodePlan.children.indices.foreach(i => {
          val childId = nextId
          nextId += 1
          buildGraph(
            graph,
            QueryPlanWithMetrics(nodePlan.children(i), node.metrics),
            QueryPlanWithMetrics(comparisonPlan.children(i), comparisonNode.metrics),
            childId);

          val color = (isGpuPlan(nodePlan), isGpuPlan(nodePlan.children(i))) match {
            case (true, true) => GPU_COLOR
            case (false, false) => CPU_COLOR
            case _ => TRANSITION_COLOR
          }
          graph.addLink(new DotLink(s"node$childId", s"node$id", color))
        })
      } else {
        // plans have diverged - cannot recurse further
        graph.addNode(new DotNode(s"node$id",
          s"plans diverge here: ${nodePlan.nodeName} vs ${comparisonPlan.nodeName}",
          "red"))
      }
    }

    val leftAlignedLabel =
      s"""
         |Application: $appId
         |Query: $sqlId
         |
         |$physicalPlanString"""
          .stripMargin
          .replace("\n", "\\l")

    val graph = new DotGraph("G", leftAlignedLabel)

    buildGraph(graph, plan, comparisonPlan.getOrElse(plan), 0)
    graph.write(fileWriter)
  }

  private def createPercentDiffString(n1: Long, n2: Long) = {
    val pct = (n2 - n1) * 100.0 / n1
    val pctStr = if (pct < 0) {
      f"$pct%.1f"
    } else {
      f"+$pct%.1f"
    }
    pctStr
  }
}

/**
 * Query plan with metrics.
 *
 * @param plan Query plan.
 * @param metrics Map of accumulatorId to metric.
 */
case class QueryPlanWithMetrics(plan: SparkPlanInfo, metrics: Map[Long, Long])