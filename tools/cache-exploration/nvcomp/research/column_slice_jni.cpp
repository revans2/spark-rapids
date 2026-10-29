/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <jni.h>

#include <cudf/column/column_view.hpp>
#include <cudf/contiguous_split.hpp>
#include <cudf/table/table_view.hpp>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <span>
#include <vector>

namespace cudf {
table_view unpack(std::span<std::uint8_t const> metadata, std::uint8_t const* data);
}

namespace {
void require(bool value, char const* message)
{
  if (!value) throw std::invalid_argument(message);
}

std::int64_t offset(void const* pointer, std::uintptr_t base, std::int64_t length)
{
  if (!pointer) return -1;
  auto address = reinterpret_cast<std::uintptr_t>(pointer);
  require(address >= base && address - base < static_cast<std::uint64_t>(length),
          "Column pointer outside packed allocation");
  return static_cast<std::int64_t>(address - base);
}

struct span {
  int ordinal;
  std::int64_t start;
  std::int64_t length;
};

void check_end(std::int64_t begin, std::int64_t bytes, span const& column)
{
  require(begin >= column.start && begin <= column.start + column.length &&
            bytes >= 0 && bytes <= column.start + column.length - begin,
          "Column buffers do not form a disjoint contiguous interval");
}
}  // namespace

// Avoid inheriting libcudf's cuDF-Java registration entry point.
extern "C" JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM*, void*) { return JNI_VERSION_1_8; }

// Research-only: unpacks host metadata with the real device base address. Avoid manifesting
// Java ColumnVectors, whose string buffer lengths may otherwise fetch GPU offset scalars.
extern "C" JNIEXPORT jobjectArray JNICALL
Java_cacheexploration_nvcomp_ColumnPruningProbe_describe(
  JNIEnv* env, jclass, jbyteArray java_metadata, jlong address, jlong length)
{
  try {
    require(java_metadata != nullptr && address != 0 && length > 0, "Invalid packed table");
    auto metadata_length = env->GetArrayLength(java_metadata);
    require(metadata_length > 0, "Empty packed metadata");
    std::vector<std::uint8_t> original_metadata(metadata_length);
    env->GetByteArrayRegion(java_metadata, 0, metadata_length,
                            reinterpret_cast<jbyte*>(original_metadata.data()));
    if (env->ExceptionCheck()) return nullptr;
    auto base = static_cast<std::uintptr_t>(address);
    require(static_cast<std::uint64_t>(length) <= std::numeric_limits<std::uintptr_t>::max() - base,
            "Address range overflow");
    auto table = cudf::unpack(std::span<std::uint8_t const>(original_metadata),
                              reinterpret_cast<std::uint8_t const*>(base));
    std::vector<span> columns;
    for (int i = 0; i < table.num_columns(); ++i) {
      auto const& col = table.column(i);
      auto type = col.type().id();
      require(col.size() > 0 && col.offset() == 0 &&
                (type == cudf::type_id::INT32 || type == cudf::type_id::INT64 ||
                 type == cudf::type_id::STRING),
              "Research probe supports nonempty int/long/string roots only");
      std::int64_t start = length;
      auto include = [&](void const* pointer) {
        auto pos = offset(pointer, base, length);
        if (pos >= 0) start = std::min(start, pos);
      };
      include(col.head<void>());
      include(col.null_mask());
      if (type == cudf::type_id::STRING) {
        require(col.num_children() == 1 &&
                  col.child(0).type().id() == cudf::type_id::INT32 &&
                  col.child(0).size() == col.size() + 1 &&
                  col.child(0).offset() == 0 && col.child(0).num_children() == 0,
                "Unsupported string offsets layout");
        include(col.child(0).head<void>());
      } else {
        require(col.num_children() == 0, "Unexpected fixed-width children");
      }
      require(start < length && start % 64 == 0, "Missing or unaligned root buffers");
      columns.push_back({i, start, 0});
    }
    require(!columns.empty(), "Empty packed schema");
    std::sort(columns.begin(), columns.end(), [](auto const& a, auto const& b) {
      return a.start < b.start;
    });
    require(columns.front().start == 0, "Unrecognized packed prefix");
    for (std::size_t i = 0; i < columns.size(); ++i) {
      auto end = i + 1 < columns.size() ? columns[i + 1].start : length;
      require(end > columns[i].start && end % 8 == 0, "Overlapping/unaligned column span");
      columns[i].length = end - columns[i].start;
    }
    std::vector<jlong> ranges(table.num_columns() * 2);
    std::vector<std::vector<std::uint8_t>> metadata(table.num_columns());
    for (auto const& s : columns) {
      auto const& col = table.column(s.ordinal);
      auto data = offset(col.head<void>(), base, length);
      auto mask = offset(col.null_mask(), base, length);
      if (mask >= 0) check_end(mask, ((static_cast<std::int64_t>(col.size()) + 31) / 32) * 4, s);
      if (col.type().id() == cudf::type_id::STRING) {
        auto offs = offset(col.child(0).head<void>(), base, length);
        check_end(offs, (static_cast<std::int64_t>(col.size()) + 1) * 4, s);
        if (data >= 0) {
          check_end(data, 0, s);
          // This is a validated fast path for the pinned GPU packer, not a public order promise.
          require(data <= offs, "Unrecognized chars/offsets order in GPU packed string");
        }
      } else {
        check_end(data, static_cast<std::int64_t>(col.size()) *
                          (col.type().id() == cudf::type_id::INT32 ? 4 : 8), s);
      }
      auto one = cudf::table_view(std::vector<cudf::column_view>{col});
      metadata[s.ordinal] = cudf::pack_metadata(
        one, reinterpret_cast<std::uint8_t const*>(base + s.start), s.length);
      ranges[s.ordinal * 2] = s.start;
      ranges[s.ordinal * 2 + 1] = s.length;
    }
    auto object_class = env->FindClass("java/lang/Object");
    auto bytes_class = env->FindClass("[B");
    if (!object_class || !bytes_class) return nullptr;
    auto result = env->NewObjectArray(2, object_class, nullptr);
    auto spans = env->NewLongArray(static_cast<jsize>(ranges.size()));
    auto metas = env->NewObjectArray(table.num_columns(), bytes_class, nullptr);
    if (!result || !spans || !metas) return nullptr;
    env->SetLongArrayRegion(spans, 0, static_cast<jsize>(ranges.size()), ranges.data());
    for (int i = 0; i < table.num_columns(); ++i) {
      require(metadata[i].size() <= static_cast<std::size_t>(std::numeric_limits<jsize>::max()),
              "Metadata array too large");
      auto bytes = env->NewByteArray(static_cast<jsize>(metadata[i].size()));
      if (!bytes) return nullptr;
      env->SetByteArrayRegion(bytes, 0, static_cast<jsize>(metadata[i].size()),
                              reinterpret_cast<jbyte const*>(metadata[i].data()));
      env->SetObjectArrayElement(metas, i, bytes);
      env->DeleteLocalRef(bytes);
      if (env->ExceptionCheck()) return nullptr;
    }
    env->SetObjectArrayElement(result, 0, spans);
    env->SetObjectArrayElement(result, 1, metas);
    return result;
  } catch (std::exception const& failure) {
    if (!env->ExceptionCheck()) {
      auto cls = env->FindClass("java/lang/IllegalArgumentException");
      if (cls) env->ThrowNew(cls, failure.what());
    }
    return nullptr;
  }
}
