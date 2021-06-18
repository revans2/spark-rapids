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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.ToolTextFileWriter

import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo

abstract class OccupancyTiming(
    val startTime: Long,
    val endTime: Long)

object OccupancyTiming {
  def calcLayoutSlotsNeeded[A <: OccupancyTiming](toSchedule: Iterable[A]): Int = {
    val slotsFreeUntil = ArrayBuffer[Long]()
    computeLayout(toSchedule, (_: A, _: Int) => (), false, slotsFreeUntil)
    slotsFreeUntil.length
  }

  def doLayout[A <: OccupancyTiming](
      toSchedule: Iterable[A],
      numSlots: Int)(scheduleCallback: (A, Int) => Unit): Unit = {
    val slotsFreeUntil = new Array[Long](numSlots).toBuffer
    computeLayout(toSchedule, scheduleCallback, true, slotsFreeUntil)
  }

  def computeLayout[A <: OccupancyTiming](
      toSchedule: Iterable[A],
      scheduleCallback: (A, Int) => Unit,
      errorOnMissingSlot: Boolean,
      slotsFreeUntil: mutable.Buffer[Long]): Unit = {
    toSchedule.foreach { timing =>
      val startTime = timing.startTime
      val slot = slotsFreeUntil.indices
          .find(i => startTime >= slotsFreeUntil(i))
          .getOrElse {
            if (errorOnMissingSlot) {
              throw new IllegalStateException("Not enough slots to schedule")
            } else {
              // Add a slot
              slotsFreeUntil.append(0L)
              slotsFreeUntil.length - 1
            }
          }
      slotsFreeUntil(slot) = timing.endTime
      scheduleCallback(timing, slot)
    }
  }
}

class OccupancyTaskInfo(val stageId: Int, val taskId: Long,
    startTime: Long, endTime: Long, val duration: Long)
    extends OccupancyTiming(startTime, endTime)

class OccupancyStageInfo(val stageId: Int,
    startTime: Long,
    endTime:Long,
    val duration: Long) extends OccupancyTiming(startTime, endTime)

class OccupancyJobInfo(val jobId: Int,
    startTime: Long,
    endTime: Long,
    val duration: Long) extends OccupancyTiming(startTime, endTime)

/**
 * Generates an SVG graph that is used to show cluster occupancy of tasks.
 */
object GenerateOccupancy {
  private val TASK_HEIGHT = 20
  private val HEADER_WIDTH = 200
  private val PADDING = 5
  private val FONT_SIZE = 14
  private val TITLE_HEIGHT = FONT_SIZE + (PADDING * 2)
  private val FOOTER_HEIGHT = FONT_SIZE + (PADDING * 2)
  private val MS_PER_PIXEL = 5.0

  // Generated using https://mokole.com/palette.html
  private val COLORS = Array(
    "#696969",
    "#dcdcdc",
    "#556b2f",
    "#8b4513",
    "#483d8b",
    "#008000",
    "#3cb371",
    "#008b8b",
    "#000080",
    "#800080",
    "#b03060",
    "#ff4500",
    "#ffa500",
    // Going to be used by lines/etc "#00ff00",
    "#8a2be2",
    "#00ff7f",
    "#dc143c",
    "#00ffff",
    "#00bfff",
    "#f4a460",
    "#0000ff",
    "#f08080",
    "#adff2f",
    "#da70d6",
    "#ff00ff",
    "#1e90ff",
    "#eee8aa",
    "#ffff54",
    "#ff1493",
    "#7b68ee")

  def generateFor(app: ApplicationInfo, outputDirectory: String): Unit = {
    val execHostToTaskList = new mutable.TreeMap[String, ArrayBuffer[OccupancyTaskInfo]]()
    val stageIdToColor = mutable.HashMap[Int, String]()
    var colorIndex = 0
    var minStart = Long.MaxValue
    var maxFinish = 0L
    app.runQuery(
      s"""
         | select
         | host,
         | executorId,
         | stageId,
         | taskId,
         | launchTime,
         | finishTime,
         | duration
         | from taskDF_${app.index} order by executorId, launchTime
         | """.stripMargin).collect().foreach { row =>
      val host = row.getString(0)
      val execId = row.getString(1)
      val stageId = row.getInt(2)
      val taskId = row.getLong(3)
      val launchTime = row.getLong(4)
      val finishTime = row.getLong(5)
      val duration = row.getLong(6)
      val taskInfo = new OccupancyTaskInfo(stageId, taskId, launchTime, finishTime, duration)
      val execHost = s"$execId/$host"
      execHostToTaskList.getOrElseUpdate(execHost, ArrayBuffer.empty) += taskInfo
      minStart = Math.min(launchTime, minStart)
      maxFinish = Math.max(finishTime, maxFinish)
      stageIdToColor.getOrElseUpdate(stageId, {
        val color = COLORS(colorIndex % COLORS.length)
        colorIndex += 1
        color
      })
    }

    val stageRangeInfo = execHostToTaskList.values.flatMap { taskList =>
      taskList
    }.groupBy { taskInfo =>
      taskInfo.stageId
    }.map {
      case (stageId, iter) =>
        val start = iter.map(_.startTime).min
        val end = iter.map(_.endTime).max
        new OccupancyStageInfo(stageId, start, end, end-start)
    }

    val stageInfo = app.runQuery(
      s"""
         |select
         |stageId,
         |submissionTime,
         |completionTime,
         |duration
         |from stageDF_${app.index} order by submissionTime
         |""".stripMargin).collect().map { row =>
      val stageId = row.getInt(0)
      val submissionTime = row.getLong(1)
      val completionTime = row.getLong(2)
      val duration = row.getLong(3)
      minStart = Math.min(minStart, submissionTime)
      maxFinish = Math.max(maxFinish, completionTime)
      new OccupancyStageInfo(stageId, submissionTime, completionTime, duration)
    }

    val execHostToSlots = execHostToTaskList.map {
      case (execHost, taskList) =>
        (execHost, OccupancyTiming.calcLayoutSlotsNeeded(taskList))
    }.toMap

    val jobInfo = app.runQuery(
      s"""
         |select
         |jobID,
         |startTime,
         |endTime,
         |duration
         |from jobDF_${app.index} order by startTime
         |""".stripMargin).collect().map { row =>
      val jobId = row.getInt(0)
      val startTime = row.getLong(1)
      val endTime = row.getLong(2)
      val duration = row.getLong(3)
      minStart = Math.min(minStart, startTime)
      maxFinish = Math.max(maxFinish, endTime)
      new OccupancyJobInfo(jobId, startTime, endTime, duration)
    }

    val numStageRangeSlots = OccupancyTiming.calcLayoutSlotsNeeded(stageRangeInfo)
    val numStageSlots = OccupancyTiming.calcLayoutSlotsNeeded(stageInfo)
    val numTaskSlots = execHostToSlots.values.sum
    val numJobSlots = OccupancyTiming.calcLayoutSlotsNeeded(jobInfo)

    val fileWriter = new ToolTextFileWriter(outputDirectory,
      s"${app.appId}-occupancy.svg")
    try {
      val width = (maxFinish - minStart)/MS_PER_PIXEL + HEADER_WIDTH + PADDING * 2
      val height = (numTaskSlots * TASK_HEIGHT) + TITLE_HEIGHT +
          FOOTER_HEIGHT + (numStageSlots * TASK_HEIGHT) +
          (numJobSlots * TASK_HEIGHT) +
          (numStageRangeSlots * TASK_HEIGHT) + PADDING * 2
      // scalastyle:off line.size.limit
      fileWriter.write(
        s"""<?xml version="1.0" encoding="UTF-8" standalone="no"?>
           |<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN"
           | "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">
           |<!-- Generated by Rapids Accelerator For Apache Spark Profiling Tool -->
           |<svg width="$width" height="$height"
           | xmlns="http://www.w3.org/2000/svg">
           | <title>${app.appId} OCCUPANCY</title>
           |""".stripMargin)
      fileWriter.write(s"""<text x="$PADDING" y="${TITLE_HEIGHT/2}" dominant-baseline="middle" font-family="Courier,monospace" font-size="$FONT_SIZE">${app.appId} OCCUPANCY</text>\n""")
      val taskHostExecXEnd = PADDING + HEADER_WIDTH
      var execHostYStart = PADDING + TITLE_HEIGHT
      execHostToTaskList.foreach {
        case (execHost, taskList) =>
          val numElements = execHostToSlots(execHost)
          val execHostHeight = numElements * TASK_HEIGHT
          val execHostMiddleY = execHostHeight/2 + execHostYStart
          // Draw a box for the Host
          fileWriter.write(
            s"""<rect x="$PADDING" y="$execHostYStart" width="$HEADER_WIDTH" height="$execHostHeight"
               | style="fill:white;fill-opacity:0.0;stroke:black;stroke-width:2"/>
               |<text x="${PADDING * 2}" y="$execHostMiddleY" dominant-baseline="middle"
               | font-family="Courier,monospace" font-size="$FONT_SIZE">$execHost</text>
               |""".stripMargin)
          OccupancyTiming.doLayout(taskList, numElements) {
            case (taskInfo, slot) =>
              val taskY = (slot * TASK_HEIGHT) + execHostYStart
              val taskXStart = taskHostExecXEnd + (taskInfo.startTime - minStart)/MS_PER_PIXEL
              val taskWidth = (taskInfo.endTime - taskInfo.startTime)/MS_PER_PIXEL
              val color = stageIdToColor(taskInfo.stageId)
              fileWriter.write(
                s"""<rect x="$taskXStart" y="$taskY" width="$taskWidth" height="$TASK_HEIGHT"
                   | style="fill:$color;fill-opacity:1.0;stroke:#00ff00;stroke-width:1"/>
                   |<text x="$taskXStart" y="${taskY + TASK_HEIGHT/2}"
                   | font-family="Courier,monospace" font-size="$FONT_SIZE">${taskInfo.duration} ms</text>
                   |""".stripMargin)
          }
          execHostYStart += execHostHeight
      }

      val xStart = taskHostExecXEnd
      val xEnd = taskHostExecXEnd + (maxFinish - minStart)/MS_PER_PIXEL
      val yStart = PADDING + TITLE_HEIGHT
      val yEnd = execHostYStart
      val yBottomStart = execHostYStart + FOOTER_HEIGHT
      val yBottomEnd = yBottomStart + numStageSlots * TASK_HEIGHT
      fileWriter.write(
        s"""<line x1="$xStart" y1="$yEnd" x2="$xEnd" y2="$yEnd" style="stroke:black;stroke-width:1"/>
           |<line x1="$xStart" y1="$yStart" x2="$xEnd" y2="$yStart" style="stroke:black;stroke-width:1"/>
           |<line x1="$xStart" y1="$yBottomEnd" x2="$xEnd" y2="$yBottomEnd" style="stroke:black;stroke-width:1"/>
           |<line x1="$xStart" y1="$yBottomStart" x2="$xEnd" y2="$yBottomStart" style="stroke:black;stroke-width:1"/>
           |""".stripMargin)
      (0L until (maxFinish-minStart)).by(100L).foreach { timeForTick =>
        val xTick = timeForTick/MS_PER_PIXEL + taskHostExecXEnd
        fileWriter.write(
          s"""<line x1="$xTick" y1="$yStart" x2="$xTick" y2="$yEnd"
             | style="stroke:black;stroke-width:1;opacity:0.5"/>
             |<line x1="$xTick" y1="$yBottomStart" x2="$xTick" y2="$yBottomEnd"
             | style="stroke:black;stroke-width:1;opacity:0.5"/>
             |""".stripMargin)
        if (timeForTick % 1000 == 0) {
          fileWriter.write(
            s"""<line x1="$xTick" y1="$yEnd"
               | x2="$xTick" y2="${yEnd + PADDING}"
               | style="stroke:black;stroke-width:1"/>
               |<text x="$xTick" y="${yEnd + PADDING + FONT_SIZE}"
               |font-family="Courier,monospace" font-size="$FONT_SIZE">$timeForTick ms</text>
               |""".stripMargin)
        }
      }
      // Now do the stage Slots
      val stageSlotsHeight = numStageSlots * TASK_HEIGHT
      val stageSlotYStart = yEnd + FOOTER_HEIGHT
      val stageStartMiddleY = stageSlotYStart + (stageSlotsHeight/2)
      fileWriter.write(
        s"""<rect x="$PADDING" y="$stageSlotYStart"
           | width="$HEADER_WIDTH" height="$stageSlotsHeight"
           | style="fill:white;fill-opacity:0.0;stroke:black;stroke-width:2"/>
           |<text x="${PADDING * 2}" y="$stageStartMiddleY" dominant-baseline="middle"
           | font-family="Courier,monospace" font-size="$FONT_SIZE">STAGES</text>
           |""".stripMargin)

      OccupancyTiming.doLayout(stageInfo, numStageSlots) {
        case (si, slot) =>
          val startTime = si.startTime
          val endTime = si.endTime

          val stageY = (slot * TASK_HEIGHT) + stageSlotYStart
          val stageXStart = taskHostExecXEnd + (startTime - minStart)/MS_PER_PIXEL
          val taskWidth = (endTime - startTime)/MS_PER_PIXEL
          val color = stageIdToColor(si.stageId)
          fileWriter.write(
            s"""<rect x="$stageXStart" y="$stageY" width="$taskWidth" height="$TASK_HEIGHT"
               | style="fill:$color;fill-opacity:1.0;stroke:#00ff00;stroke-width:1"/>
               |<text x="$stageXStart" y="${stageY + TASK_HEIGHT/2}" dominant-baseline="middle"
               |  font-family="Courier,monospace" font-size="$FONT_SIZE">STAGE ${si.stageId} ${si.duration} ms</text>
               |""".stripMargin)
      }

      // Now do the Job Slots
      val jobSlotsHeight = numJobSlots * TASK_HEIGHT
      val jobSlotYStart = stageSlotYStart + stageSlotsHeight
      val jobStartMiddleY = jobSlotYStart + (jobSlotsHeight/2)
      fileWriter.write(
        s"""<rect x="$PADDING" y="$jobSlotYStart"
           | width="$HEADER_WIDTH" height="$jobSlotsHeight"
           | style="fill:white;fill-opacity:0.0;stroke:black;stroke-width:2"/>
           |<text x="${PADDING * 2}" y="$jobStartMiddleY" dominant-baseline="middle"
           | font-family="Courier,monospace" font-size="$FONT_SIZE">JOBS</text>
           |""".stripMargin)

      OccupancyTiming.doLayout(jobInfo, numJobSlots) {
        case (ji, slot) =>
          val startTime = ji.startTime
          val endTime = ji.endTime

          val jobY = (slot * TASK_HEIGHT) + jobSlotYStart
          val jobXStart = taskHostExecXEnd + (startTime - minStart)/MS_PER_PIXEL
          val taskWidth = (endTime - startTime)/MS_PER_PIXEL
          val color = "green" // TODO stageIdToColor(si.stageId)
          fileWriter.write(
            s"""<rect x="$jobXStart" y="$jobY" width="$taskWidth" height="$TASK_HEIGHT"
               | style="fill:$color;fill-opacity:1.0;stroke:#00ff00;stroke-width:1"/>
               |<text x="$jobXStart" y="${jobY + TASK_HEIGHT/2}" dominant-baseline="middle"
               |  font-family="Courier,monospace" font-size="$FONT_SIZE">JOB ${ji.jobId} ${ji.duration} ms</text>
               |""".stripMargin)
      }

      // Now do the stage Range Slots
      val stageRangeSlotsHeight = numStageRangeSlots * TASK_HEIGHT
      val stageRangeSlotYStart = jobSlotYStart + jobSlotsHeight
      val stageRangeStartMiddleY = stageRangeSlotYStart + (stageRangeSlotsHeight/2)
      fileWriter.write(
        s"""<rect x="$PADDING" y="$stageRangeSlotYStart"
           | width="$HEADER_WIDTH" height="$stageRangeSlotsHeight"
           | style="fill:white;fill-opacity:0.0;stroke:black;stroke-width:2"/>
           |<text x="${PADDING * 2}" y="$stageRangeStartMiddleY" dominant-baseline="middle"
           | font-family="Courier,monospace" font-size="$FONT_SIZE">STAGE RANGES</text>
           |""".stripMargin)

      OccupancyTiming.doLayout(stageRangeInfo, numStageRangeSlots) {
        case (si, slot) =>
          val startTime = si.startTime
          val endTime = si.endTime

          val stageRangeY = (slot * TASK_HEIGHT) + stageRangeSlotYStart
          val stageRangeXStart = taskHostExecXEnd + (startTime - minStart)/MS_PER_PIXEL
          val taskWidth = (endTime - startTime)/MS_PER_PIXEL
          val color = stageIdToColor(si.stageId)
          fileWriter.write(
            s"""<rect x="$stageRangeXStart" y="$stageRangeY" width="$taskWidth" height="$TASK_HEIGHT"
               | style="fill:$color;fill-opacity:1.0;stroke:#00ff00;stroke-width:1"/>
               |<text x="$stageRangeXStart" y="${stageRangeY + TASK_HEIGHT/2}" dominant-baseline="middle"
               |  font-family="Courier,monospace" font-size="$FONT_SIZE">STAGE RANGE ${si.stageId} ${si.duration} ms</text>
               |""".stripMargin)
      }

      fileWriter.write(s"""</svg>""")
      // scalastyle:on line.size.limit
    } finally {
      fileWriter.close()
    }
  }
}
