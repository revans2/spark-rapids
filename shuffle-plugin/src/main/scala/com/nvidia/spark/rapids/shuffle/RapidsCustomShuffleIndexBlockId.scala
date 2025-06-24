/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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
package com.nvidia.spark.rapids.shuffle

import org.apache.spark.storage.BlockId

case class RapidsCustomShuffleIndexBlockId(
    shuffleId: Int,
    mapId: Long,
    attemptId: Long) extends BlockId {
  override def name: String = s"rapids_custom_shuffle_index_${shuffleId}_${mapId}_${attemptId}"
}
