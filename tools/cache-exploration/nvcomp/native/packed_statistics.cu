/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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
#include <jni.h>
#include <cuda_runtime.h>
#include <cub/block/block_reduce.cuh>

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>

// All structures belong to this extension. No cuDF structures or private symbols cross its ABI.
namespace {
constexpr int block_size = 256;
constexpr int tile_rows = 4096;

struct Descriptor {
  int64_t type, rows, declared_nulls, data, mask, offsets, bytes, base, tile_start, tile_count;
};
struct Result {
  int64_t nulls, minimum, maximum, min_offset, min_length, max_offset, max_length, error;
};
struct Copy {
  int64_t base, source, length, destination;
};
static_assert(sizeof(Descriptor) == 10 * sizeof(int64_t));
static_assert(sizeof(Result) == 8 * sizeof(int64_t));
static_assert(sizeof(Copy) == 4 * sizeof(int64_t));

__device__ Result empty_result()
{
  return {0, INT64_MAX, INT64_MIN, -1, -1, -1, -1, 0};
}

__device__ uint8_t const* base(Descriptor const& d)
{
  return reinterpret_cast<uint8_t const*>(static_cast<uintptr_t>(d.base));
}

// Spark UTF8String uses unsigned binary lexicographic ordering, including prefix lengths.
__device__ int compare(Descriptor const& d, int64_t left, int64_t left_length,
                       int64_t right, int64_t right_length)
{
  auto const* chars = base(d);
  if (left == right && left_length == right_length) return 0;
  auto count = left_length < right_length ? left_length : right_length;
  for (int64_t i = 0; i < count; ++i) {
    if (chars[left + i] != chars[right + i]) {
      return chars[left + i] < chars[right + i] ? -1 : 1;
    }
  }
  return left_length < right_length ? -1 : (left_length > right_length ? 1 : 0);
}

struct Merge {
  Descriptor d;
  __device__ Result operator()(Result a, Result const& b) const
  {
    a.nulls += b.nulls;
    a.error |= b.error;
    if (b.min_length < 0) return a;
    if (a.min_length < 0) {
      auto nulls = a.nulls;
      auto error = a.error;
      a = b;
      a.nulls = nulls;
      a.error = error;
      return a;
    }
    if (d.type == 23) {
      if (compare(d, b.min_offset, b.min_length, a.min_offset, a.min_length) < 0) {
        a.min_offset = b.min_offset;
        a.min_length = b.min_length;
      }
      if (compare(d, b.max_offset, b.max_length, a.max_offset, a.max_length) > 0) {
        a.max_offset = b.max_offset;
        a.max_length = b.max_length;
      }
    } else {
      if (b.minimum < a.minimum) a.minimum = b.minimum;
      if (b.maximum > a.maximum) a.maximum = b.maximum;
    }
    return a;
  }
};

__device__ int find_column(Descriptor const* columns, int count, int tile)
{
  int low = 0;
  int high = count;
  while (low + 1 < high) {
    int mid = low + (high - low) / 2;
    if (columns[mid].tile_start <= tile) low = mid;
    else high = mid;
  }
  return low;
}

__global__ void partial_statistics(Descriptor const* columns, int count, Result* output)
{
  using Reduce = cub::BlockReduce<Result, block_size>;
  __shared__ typename Reduce::TempStorage storage;
  auto d = columns[find_column(columns, count, blockIdx.x)];
  int64_t first = (static_cast<int64_t>(blockIdx.x) - d.tile_start) * tile_rows;
  int64_t end = first + tile_rows < d.rows ? first + tile_rows : d.rows;
  Result state = empty_result();
  Merge merge{d};
  for (int64_t row = first + threadIdx.x; row < end; row += block_size) {
    if (d.mask != -1) {
      auto words = reinterpret_cast<uint32_t const*>(base(d) + d.mask);
      if ((words[row / 32] & (uint32_t{1} << (row % 32))) == 0) {
        ++state.nulls;
        continue;
      }
    }
    Result element = empty_result();
    if (d.type == 23) {
      auto offsets = reinterpret_cast<int32_t const*>(base(d) + d.offsets);
      int64_t start = offsets[row];
      int64_t finish = offsets[row + 1];
      // An absent chars buffer is valid only for zero-length strings.
      if (start < 0 || finish < start ||
          (d.data == -1 ? finish != 0 : finish > d.bytes - d.data)) {
        state.error |= 1;
        continue;
      }
      element.min_offset = element.max_offset = d.data == -1 ? 0 : d.data + start;
      element.min_length = element.max_length = finish - start;
      element.minimum = element.maximum = 0;
    } else {
      int64_t value = d.type == 3
        ? static_cast<int64_t>(reinterpret_cast<int32_t const*>(base(d) + d.data)[row])
        : reinterpret_cast<int64_t const*>(base(d) + d.data)[row];
      element.minimum = element.maximum = value;
      element.min_offset = element.max_offset = 0;
      element.min_length = element.max_length = 0;
    }
    state = merge(state, element);
  }
  Result result = Reduce(storage).Reduce(state, merge);
  if (threadIdx.x == 0) output[blockIdx.x] = result;
}

__global__ void merged_statistics(Descriptor const* columns, Result const* partials,
                                   Result* output)
{
  using Reduce = cub::BlockReduce<Result, block_size>;
  __shared__ typename Reduce::TempStorage storage;
  auto d = columns[blockIdx.x];
  Merge merge{d};
  Result state = empty_result();
  for (int64_t tile = threadIdx.x; tile < d.tile_count; tile += block_size) {
    state = merge(state, partials[d.tile_start + tile]);
  }
  Result result = Reduce(storage).Reduce(state, merge);
  if (threadIdx.x == 0) {
    if (d.type == 23 && result.min_length >= 0 &&
        compare(d, result.min_offset, result.min_length,
                    result.max_offset, result.max_length) == 0) {
      // Equal strings can occupy different rows: deduplicate by value, not pointer identity.
      result.max_offset = result.min_offset;
      result.max_length = result.min_length;
    }
    if (result.nulls != d.declared_nulls) result.error |= 2;
    output[blockIdx.x] = result;
  }
}

__global__ void gather_strings(Copy const* copies, int count, int64_t bytes, uint8_t* output)
{
  // A flat byte grid keeps even a single very long bound parallel across many blocks.
  int64_t at = static_cast<int64_t>(blockIdx.x) * block_size + threadIdx.x;
  if (at >= bytes) return;
  int low = 0;
  int high = count;
  while (low + 1 < high) {
    int mid = low + (high - low) / 2;
    if (copies[mid].destination <= at) low = mid;
    else high = mid;
  }
  auto copy = copies[low];
  if (at < copy.destination + copy.length) {
    auto pointer = reinterpret_cast<uint8_t const*>(static_cast<uintptr_t>(copy.base));
    output[at] = pointer[copy.source + at - copy.destination];
  }
}

void checked(cudaError_t status, char const* stage)
{
  if (status != cudaSuccess) {
    throw std::runtime_error(std::string(stage) + ": " + cudaGetErrorString(status));
  }
}

void failure(JNIEnv* env, std::exception const& error)
{
  env->ThrowNew(env->FindClass("java/lang/IllegalStateException"), error.what());
}

void set_device(int device)
{
  if (device < 0) throw std::runtime_error("Invalid packed statistics CUDA device");
  checked(cudaSetDevice(device), "Set packed statistics device");
}

}  // namespace

extern "C" JNIEXPORT void JNICALL
Java_org_apache_spark_sql_cacheexploration_GpuPackedStatistics_reduce(
    JNIEnv* env, jclass, jlong descriptors, jint columns, jint tiles, jlong scratch,
    jlong output, jlong raw_stream, jint device)
{
  try {
    if (columns <= 0 || tiles <= 0 || !descriptors || !scratch || !output) {
      throw std::runtime_error("Invalid packed statistics launch");
    }
    set_device(device);
    // Java supplies actual CUDA stream IDs, including legacy=1 and PTDS=2.
    auto stream = reinterpret_cast<cudaStream_t>(raw_stream);
    auto desc = reinterpret_cast<Descriptor const*>(descriptors);
    auto partial = reinterpret_cast<Result*>(scratch);
    partial_statistics<<<tiles, block_size, 0, stream>>>(desc, columns, partial);
    checked(cudaGetLastError(), "Launch packed partial statistics");
    merged_statistics<<<columns, block_size, 0, stream>>>(
        desc, partial, reinterpret_cast<Result*>(output));
    checked(cudaGetLastError(), "Launch packed merged statistics");
  } catch (std::exception const& error) {
    failure(env, error);
  }
}

extern "C" JNIEXPORT void JNICALL
Java_org_apache_spark_sql_cacheexploration_GpuPackedStatistics_gather(
    JNIEnv* env, jclass, jlong descriptors, jint copies, jlong bytes, jlong output,
    jlong raw_stream, jint device)
{
  try {
    if (copies <= 0 || bytes <= 0 || !descriptors || !output ||
        static_cast<uint64_t>(bytes) >
          static_cast<uint64_t>(std::numeric_limits<int>::max()) * block_size) {
      throw std::runtime_error("Invalid packed string gather launch");
    }
    set_device(device);
    auto stream = reinterpret_cast<cudaStream_t>(raw_stream);
    auto blocks = static_cast<unsigned int>((bytes - 1) / block_size + 1);
    gather_strings<<<blocks, block_size, 0, stream>>>(
        reinterpret_cast<Copy const*>(descriptors), copies, bytes,
        reinterpret_cast<uint8_t*>(output));
    checked(cudaGetLastError(), "Launch packed string gather");
  } catch (std::exception const& error) {
    failure(env, error);
  }
}
