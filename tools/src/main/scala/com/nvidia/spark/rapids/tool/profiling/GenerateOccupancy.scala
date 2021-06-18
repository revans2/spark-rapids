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

  private def textBoxVirtCentered(
      text: String,
      x: Number,
      y: Long,
      fileWriter: ToolTextFileWriter): Unit =
    fileWriter.write(
      s"""<text x="$x" y="$y" dominant-baseline="middle"
         | font-family="Courier,monospace" font-size="$FONT_SIZE">$text</text>
         |""".stripMargin)

  private def titleBox(
      text: String,
      yStart: Long,
      numElements: Int,
      fileWriter: ToolTextFileWriter): Int = {
    val boxHeight = numElements * TASK_HEIGHT
    val boxMiddleY = boxHeight/2 + yStart
    // Draw a box for the Host
    fileWriter.write(
      s"""<rect x="$PADDING" y="$yStart" width="$HEADER_WIDTH" height="$boxHeight"
         | style="fill:white;fill-opacity:0.0;stroke:black;stroke-width:2"/>
         |""".stripMargin)
    textBoxVirtCentered(text, PADDING * 2, boxMiddleY, fileWriter)
    boxHeight
  }

  private def timingBox[A <: OccupancyTiming](
      text: String,
      color: String,
      timing: A,
      slot: Int,
      xStart: Long,
      yStart: Long,
      minStart: Long,
      fileWriter: ToolTextFileWriter): Unit = {
    val startTime = timing.startTime
    val endTime = timing.endTime
    val x = xStart + (startTime - minStart)/MS_PER_PIXEL
    val y = (slot * TASK_HEIGHT) + yStart
    val width = (endTime - startTime)/MS_PER_PIXEL
    fileWriter.write(
      s"""<rect x="$x" y="$y" width="$width" height="$TASK_HEIGHT"
         | style="fill:$color;fill-opacity:1.0;stroke:#00ff00;stroke-width:1"/>
         |""".stripMargin)
    textBoxVirtCentered(text, x, y + TASK_HEIGHT/2, fileWriter)
  }

  private def scaleWithLines(x: Long,
      y: Long,
      minStart: Long,
      maxFinish: Long,
      height: Long,
      fileWriter: ToolTextFileWriter): Unit = {
    val timeRange = maxFinish - minStart
    val xEnd = x + timeRange/MS_PER_PIXEL
    val yEnd = y + height
    fileWriter.write(
      s"""<line x1="$x" y1="$yEnd" x2="$xEnd" y2="$yEnd" style="stroke:black;stroke-width:1"/>
         |<line x1="$x" y1="$y" x2="$xEnd" y2="$y" style="stroke:black;stroke-width:1"/>
         |""".stripMargin)
    (0L until timeRange).by(100L).foreach { timeForTick =>
      val xTick = timeForTick/MS_PER_PIXEL + x
      fileWriter.write(
        s"""<line x1="$xTick" y1="$y" x2="$xTick" y2="$yEnd"
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
  }

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
        (execHost, calcLayoutSlotsNeeded(taskList))
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

    val numStageRangeSlots = calcLayoutSlotsNeeded(stageRangeInfo)
    val numStageSlots = calcLayoutSlotsNeeded(stageInfo)
    val numTaskSlots = execHostToSlots.values.sum
    val numJobSlots = calcLayoutSlotsNeeded(jobInfo)

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
      // TITLE AT TOP
      textBoxVirtCentered(s"${app.appId} OCCUPANCY", PADDING, TITLE_HEIGHT/2, fileWriter)

      val taskHostExecXEnd = PADDING + HEADER_WIDTH
      var execHostYStart = PADDING + TITLE_HEIGHT
      execHostToTaskList.foreach {
        case (execHost, taskList) =>
          val numElements = execHostToSlots(execHost)
          val execHostHeight = numElements * TASK_HEIGHT

          titleBox(execHost, execHostYStart, numElements, fileWriter)
          doLayout(taskList, numElements) {
            case (taskInfo, slot) =>
              timingBox(s"${taskInfo.duration} ms",
                stageIdToColor(taskInfo.stageId),
                taskInfo,
                slot,
                taskHostExecXEnd,
                execHostYStart,
                minStart,
                fileWriter)
          }
          execHostYStart += execHostHeight
      }

      scaleWithLines(taskHostExecXEnd,
        PADDING + TITLE_HEIGHT,
        minStart,
        maxFinish,
        execHostYStart - (PADDING + TITLE_HEIGHT),
        fileWriter)

      val yEnd = execHostYStart

      // Now do the stage Slots
      val stageSlotsHeight = numStageSlots * TASK_HEIGHT
      val stageSlotYStart = yEnd + FOOTER_HEIGHT
      titleBox("STAGES", stageSlotYStart, numStageSlots, fileWriter)

      doLayout(stageInfo, numStageSlots) {
        case (si, slot) =>
          timingBox(s"STAGE ${si.stageId} ${si.duration} ms",
            stageIdToColor(si.stageId),
            si,
            slot,
            taskHostExecXEnd,
            stageSlotYStart,
            minStart,
            fileWriter)
      }

      // Now do the Job Slots
      val jobSlotsHeight = numJobSlots * TASK_HEIGHT
      val jobSlotYStart = stageSlotYStart + stageSlotsHeight
      titleBox("JOBS", jobSlotYStart, numJobSlots, fileWriter)

      doLayout(jobInfo, numJobSlots) {
        case (ji, slot) =>
          timingBox(s"JOB ${ji.jobId} ${ji.duration} ms",
            "green", // TODO select unique colors???
            ji,
            slot,
            taskHostExecXEnd,
            jobSlotYStart,
            minStart,
            fileWriter)
      }

      // Now do the stage Range Slots
      val stageRangeSlotsHeight = numStageRangeSlots * TASK_HEIGHT
      val stageRangeSlotYStart = jobSlotYStart + jobSlotsHeight
      titleBox("STAGE RANGES", stageRangeSlotYStart, numStageRangeSlots, fileWriter)

      doLayout(stageRangeInfo, numStageRangeSlots) {
        case (si, slot) =>
          timingBox(s"STAGE RANGE ${si.stageId} ${si.duration} ms",
            stageIdToColor(si.stageId),
            si,
            slot,
            taskHostExecXEnd,
            stageRangeSlotYStart,
            minStart,
            fileWriter)
      }

      fileWriter.write(s"""</svg>""")
      // scalastyle:on line.size.limit
    } finally {
      fileWriter.close()
    }
  }
}
