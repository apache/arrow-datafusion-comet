/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.comet.execution.arrow

import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.vector.CometVector

object CometArrowConverters extends Logging {
  // this is similar how Spark converts internal row to Arrow format except that we are transforming
  // the result batch to Comet's Internal ColumnarBatch instead of serialized bytes.
  private[sql] class ArrowBatchIterator(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      maxRecordsPerBatch: Long,
      timeZoneId: String,
      context: TaskContext)
      extends Iterator[ColumnarBatch]
      with AutoCloseable {

    // todo: hmm, we need to handle arrow shading problem, as Spark may use a different version of
    //    arrow.
    private val arrowSchema =
      ArrowUtils.toArrowSchema(schema, timeZoneId)
    // Reuse the same root allocator here, maybe we should also reuse the same allocator in the
    // comet code base
    private val allocator =
      ArrowUtils.rootAllocator.newChildAllocator(
        s"to${this.getClass.getSimpleName}",
        0,
        Long.MaxValue)

    private val root = VectorSchemaRoot.create(arrowSchema, allocator)
    private val arrowWriter = ArrowWriter.create(root)
    private var columnarBatch: ColumnarBatch = null

    Option(context).foreach {
      _.addTaskCompletionListener[Unit] { _ =>
        close()
      }
    }

    override def hasNext: Boolean = rowIter.hasNext || {
      close()
      false
    }

    override def next(): ColumnarBatch = {
      if (columnarBatch != null) {
        // reset the arrowWrite and columnarBatch. The reset method is called only after the
        // columnarBatch is consumed by the caller.
        arrowWriter.reset()
        columnarBatch = null
      }
      var rowCount = 0L
      while (rowIter.hasNext && (maxRecordsPerBatch <= 0 || rowCount < maxRecordsPerBatch)) {
        val row = rowIter.next()
        arrowWriter.write(row)
        rowCount += 1
      }
      arrowWriter.finish()
      columnarBatch = wrapperFor(root)
      columnarBatch
    }

    override def close(): Unit = {
      if (columnarBatch != null) {
        arrowWriter.reset()
        columnarBatch = null
      }
      root.close()
      allocator.close()
    }
  }

  private def wrapperFor(root: VectorSchemaRoot) = {
    val columns = root.getFieldVectors.asScala.map { vector =>
      CometVector.getVector(vector, false)
    }
    new ColumnarBatch(columns.toArray, root.getRowCount)
  }

  def toArrowBatchIterator(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      maxRecordsPerBatch: Long,
      timeZoneId: String,
      context: TaskContext): Iterator[ColumnarBatch] = {
    new ArrowBatchIterator(rowIter, schema, maxRecordsPerBatch, timeZoneId, context)
  }
}
