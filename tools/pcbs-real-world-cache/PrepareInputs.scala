/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations.
 */
import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetFileWriter}
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.MessageType

/** Research preparation only: copies encoded row groups; does not create a Spark context. */
object PrepareInputs {
  // ParquetFileWriter has no public close; expose its protected output for failure cleanup.
  private class CopyWriter(conf: Configuration, schema: MessageType, path: Path)
      extends ParquetFileWriter(conf, schema, path, ParquetFileWriter.Mode.CREATE)
      with AutoCloseable {
    override def close(): Unit = out.close()
  }

  def main(args: Array[String]): Unit = {
    require(args.length == 1, "Expected immutable preparation plan JSON")
    val mapper = new ObjectMapper()
    val plan = mapper.readTree(Files.readAllBytes(Paths.get(args(0))))
    val conf = new Configuration()
    val sources = plan.get("sources").elements().asScala.map(_.asText()).toVector
    val footers: Vector[ParquetMetadata] = sources.map { source =>
      val reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(source), conf))
      try reader.getFooter finally reader.close()
    }
    require(footers.nonEmpty)
    val schema = footers.head.getFileMetaData.getSchema
    require(footers.forall(_.getFileMetaData.getSchema == schema),
      "Physical Parquet schemas must be identical")
    val partitions = plan.get("partitions").elements().asScala.toVector
    require(partitions.length == 16)
    partitions.foreach { partition =>
      val groups = partition.get("groups").elements().asScala.toVector
      require(groups.nonEmpty, "Every canonical partition must contain a row group")
      val firstSource = groups.head.get("source_index").asInt()
      val metadata = footers(firstSource).getFileMetaData.getKeyValueMetaData
      require(groups.forall { group =>
        footers(group.get("source_index").asInt()).getFileMetaData
          .getKeyValueMetaData == metadata
      }, "Partition crosses file metadata variants")
      val writer = new CopyWriter(conf, schema, new Path(partition.get("path").asText()))
      try {
        writer.start()
        groups.groupBy(_.get("source_index").asInt()).toVector.sortBy(_._1).foreach {
          case (sourceIndex, sourceGroups) =>
            val input = HadoopInputFile.fromPath(new Path(sources(sourceIndex)), conf).newStream()
            try {
              val ordered = sourceGroups.map { group =>
                footers(sourceIndex).getBlocks.get(group.get("source_row_group").asInt())
              }
              // API copies raw encoded chunks and adjusts page offsets.
              // Parquet-MR1.13.1 intentionally omits column/offset page indexes here.
              writer.appendRowGroups(input, ordered.asJava, false)
            } finally {
              input.close()
            }
        }
        writer.end(metadata)
      } finally {
        writer.close()
      }
    }
    println("PREPARE_INPUTS COPY PASS")
  }
}
