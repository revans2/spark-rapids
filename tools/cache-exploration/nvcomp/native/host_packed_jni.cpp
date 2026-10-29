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
#include <cstring>
#include <limits>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

// The pinned library exports the bounds-checked overload. Its declaration is absent from the
// mounted public header; use that exported host API rather than reproduce opaque metadata bytes.
namespace cudf {
table_view unpack(std::span<std::uint8_t const> metadata, std::uint8_t const* data);
}

namespace {

void require(bool condition, char const* message)
{
  if (!condition) throw std::invalid_argument(message);
}

template <typename T, typename Array>
std::vector<T> read_array(JNIEnv* env, Array array)
{
  require(array != nullptr, "Null descriptor array");
  auto length = env->GetArrayLength(array);
  std::vector<T> values(length);
  if constexpr (sizeof(T) == sizeof(jint)) {
    env->GetIntArrayRegion(array, 0, length, values.data());
  } else {
    env->GetLongArrayRegion(array, 0, length, values.data());
  }
  if (env->ExceptionCheck()) throw std::runtime_error("Cannot read descriptors");
  return values;
}

struct pinned_bytes {
  JNIEnv* env;
  jbyteArray array;
  std::uint8_t const* data;
  std::size_t length;

  pinned_bytes(JNIEnv* env, jbyteArray array) : env(env), array(array), data(nullptr), length(0)
  {
    require(array != nullptr, "Null packed byte array");
    length = env->GetArrayLength(array);
    require(length > 0, "Empty packed data is not supported by the host bridge");
    data = static_cast<std::uint8_t const*>(env->GetPrimitiveArrayCritical(array, nullptr));
    if (!data) throw std::bad_alloc();
  }

  ~pinned_bytes()
  {
    if (data) env->ReleasePrimitiveArrayCritical(array, const_cast<std::uint8_t*>(data), JNI_ABORT);
  }
  pinned_bytes(pinned_bytes const&) = delete;
  pinned_bytes& operator=(pinned_bytes const&) = delete;
};

void check_buffer(jlong offset, std::uint64_t bytes, std::size_t packed_size, std::size_t alignment)
{
  require(offset >= 0, "Required column buffer is absent");
  auto start = static_cast<std::uint64_t>(offset);
  require(start % alignment == 0, "Misaligned packed column buffer");
  require(start <= packed_size && bytes <= packed_size - start, "Column buffer exceeds packed data");
}

std::int32_t load_offset(std::uint8_t const* bytes, std::size_t at)
{
  std::int32_t value;
  std::memcpy(&value, bytes + at, sizeof(value));
  return value;
}

void check_strings(std::uint8_t const* base, std::size_t packed_size, int rows,
                   jlong data_offset, jlong offsets_offset)
{
  check_buffer(offsets_offset, (static_cast<std::uint64_t>(rows) + 1) * 4, packed_size, 4);
  auto start = static_cast<std::size_t>(offsets_offset);
  require(load_offset(base, start) == 0, "String offsets must start at zero");
  std::int32_t previous = 0;
  for (int row = 1; row <= rows; ++row) {
    auto next = load_offset(base, start + static_cast<std::size_t>(row) * 4);
    require(next >= previous, "String offsets must be nonnegative and ordered");
    previous = next;
  }
  if (previous == 0 && data_offset == -1) return;
  check_buffer(data_offset, static_cast<std::uint64_t>(previous), packed_size, 1);
}

jlong pointer_offset(void const* pointer, std::uint8_t const* base, std::size_t length)
{
  if (!pointer) return -1;
  auto address = reinterpret_cast<std::uintptr_t>(pointer);
  auto start = reinterpret_cast<std::uintptr_t>(base);
  require(address >= start && address - start <= length, "Metadata pointer outside packed data");
  return static_cast<jlong>(address - start);
}

void check_column(int type, int rows, int nulls, jlong data, jlong mask, jlong offsets,
                  std::uint8_t const* base, std::size_t bytes)
{
  require(rows > 0 && nulls >= 0 && nulls <= rows, "Invalid row/null count");
  require(type == 3 || type == 4 || type == 23, "Host packed bridge supports int/long/string only");
  if (mask == -1) {
    require(nulls == 0, "Nullable column lacks validity buffer");
  } else {
    // cuDF masks consist of uint32 words, including the tail word.
    check_buffer(mask, ((static_cast<std::uint64_t>(rows) + 31) / 32) * 4, bytes, 4);
    require((reinterpret_cast<std::uintptr_t>(base + mask) % 4) == 0,
            "Validity buffer address is not uint32-aligned");
  }
  if (type == 23) {
    check_strings(base, bytes, rows, data, offsets);
  } else {
    require(offsets == -1, "Fixed-width column unexpectedly has string offsets");
    check_buffer(data, static_cast<std::uint64_t>(rows) * (type == 3 ? 4 : 8), bytes,
                 type == 3 ? 4 : 8);
  }
}


std::vector<std::uint8_t> read_metadata(JNIEnv* env, jbyteArray array)
{
  require(array != nullptr, "Null packed metadata");
  auto size = env->GetArrayLength(array);
  require(size > 0, "Empty packed metadata");
  std::vector<std::uint8_t> bytes(size);
  env->GetByteArrayRegion(array, 0, size, reinterpret_cast<jbyte*>(bytes.data()));
  if (env->ExceptionCheck()) throw std::runtime_error("Cannot read packed metadata");
  return bytes;
}

jbyteArray metadata_array(JNIEnv* env, std::vector<std::uint8_t> const& metadata)
{
  require(metadata.size() <= static_cast<std::size_t>(std::numeric_limits<jsize>::max()),
          "Metadata exceeds Java array length");
  auto result = env->NewByteArray(static_cast<jsize>(metadata.size()));
  if (result) env->SetByteArrayRegion(result, 0, static_cast<jsize>(metadata.size()),
                                     reinterpret_cast<jbyte const*>(metadata.data()));
  return result;
}

// Validate metadata-derived spans without dereferencing the possibly-device pointers.
void check_span(cudf::column_view const& col, std::uint8_t const* base,
                std::size_t length, bool gpu_order)
{
  auto type = static_cast<int>(col.type().id());
  require(col.size() > 0 && col.offset() == 0 && col.null_count() >= 0 &&
            col.null_count() <= col.size(), "Invalid packed row/null count");
  require(type == 3 || type == 4 || type == 23, "Packed index supports int/long/string only");
  auto data = pointer_offset(col.head<void>(), base, length);
  auto mask = pointer_offset(col.null_mask(), base, length);
  if (mask == -1) require(col.null_count() == 0, "Nullable column lacks validity buffer");
  else check_buffer(mask, ((static_cast<std::uint64_t>(col.size()) + 31) / 32) * 4,
                    length, 4);
  if (type == 23) {
    require(col.num_children() == 1, "Expected one string offsets child");
    auto const& child = col.child(0);
    require(child.type().id() == cudf::type_id::INT32 &&
              static_cast<std::int64_t>(child.size()) == static_cast<std::int64_t>(col.size()) + 1 &&
              child.offset() == 0 && child.null_count() == 0 && child.num_children() == 0,
            "Invalid string offsets child");
    auto offsets = pointer_offset(child.head<void>(), base, length);
    check_buffer(offsets, (static_cast<std::uint64_t>(col.size()) + 1) * 4, length, 4);
    if (data != -1) {
      check_buffer(data, 0, length, 1);
      // Validated for this pinned GPU packer only; CPU layout stores offsets before chars.
      if (gpu_order) require(data <= offsets, "Unrecognized GPU chars/offsets order");
    }
  } else {
    require(col.num_children() == 0, "Unexpected fixed-width children");
    check_buffer(data, static_cast<std::uint64_t>(col.size()) * (type == 3 ? 4 : 8),
                 length, type == 3 ? 4 : 8);
  }
}

struct column_index {
  std::vector<jlong> ranges;
  std::vector<std::vector<std::uint8_t>> metadata;
};

column_index index_columns(cudf::table_view const& table, std::uint8_t const* base,
                           std::size_t length, bool host)
{
  require(table.num_columns() > 0 && table.num_rows() > 0, "Empty packed table");
  require(static_cast<std::uint64_t>(table.num_columns()) * 2 <=
            std::numeric_limits<jsize>::max(), "Too many packed columns");
  struct column_span { int ordinal; jlong start; };
  std::vector<column_span> spans;
  for (int i = 0; i < table.num_columns(); ++i) {
    auto const& col = table.column(i);
    check_span(col, base, length, !host);
    jlong start = static_cast<jlong>(length);
    auto include = [&](void const* pointer) {
      auto pos = pointer_offset(pointer, base, length);
      if (pos >= 0) start = std::min(start, pos);
    };
    include(col.head<void>());
    include(col.null_mask());
    if (col.type().id() == cudf::type_id::STRING) include(col.child(0).head<void>());
    require(start < static_cast<jlong>(length) && start % 64 == 0,
            "Missing or unaligned packed root buffers");
    spans.push_back({i, start});
  }
  std::sort(spans.begin(), spans.end(), [](auto const& a, auto const& b) {
    return a.start < b.start;
  });
  require(spans.front().start == 0, "Unrecognized packed prefix");
  column_index result;
  result.ranges.resize(static_cast<std::size_t>(table.num_columns()) * 2);
  result.metadata.resize(table.num_columns());
  for (std::size_t i = 0; i < spans.size(); ++i) {
    auto const& s = spans[i];
    auto end = i + 1 < spans.size() ? spans[i + 1].start : static_cast<jlong>(length);
    require(end > s.start && end % (host ? 64 : 8) == 0,
            "Overlapping or unaligned packed column span");
    auto bytes = static_cast<std::size_t>(end - s.start);
    auto column_base = base + s.start;
    auto const& col = table.column(s.ordinal);
    check_span(col, column_base, bytes, !host);
    if (host) {
      auto type = static_cast<int>(col.type().id());
      check_column(type, col.size(), col.null_count(),
                   pointer_offset(col.head<void>(), column_base, bytes),
                   pointer_offset(col.null_mask(), column_base, bytes),
                   type == 23 ? pointer_offset(col.child(0).head<void>(), column_base, bytes) : -1,
                   column_base, bytes);
    }
    result.ranges[s.ordinal * 2] = s.start;
    result.ranges[s.ordinal * 2 + 1] = static_cast<jlong>(bytes);
    result.metadata[s.ordinal] = cudf::pack_metadata(
      cudf::table_view(std::vector<cudf::column_view>{col}), column_base, bytes);
  }
  return result;
}

jobjectArray index_array(JNIEnv* env, column_index const& index)
{
  auto object_class = env->FindClass("java/lang/Object");
  auto bytes_class = env->FindClass("[B");
  if (!object_class || !bytes_class) return nullptr;
  auto result = env->NewObjectArray(2, object_class, nullptr);
  auto ranges = env->NewLongArray(static_cast<jsize>(index.ranges.size()));
  auto metadata = env->NewObjectArray(static_cast<jsize>(index.metadata.size()), bytes_class, nullptr);
  if (!result || !ranges || !metadata) return nullptr;
  env->SetLongArrayRegion(ranges, 0, static_cast<jsize>(index.ranges.size()), index.ranges.data());
  for (std::size_t i = 0; i < index.metadata.size(); ++i) {
    auto bytes = metadata_array(env, index.metadata[i]);
    if (!bytes) return nullptr;
    env->SetObjectArrayElement(metadata, static_cast<jsize>(i), bytes);
    env->DeleteLocalRef(bytes);
    if (env->ExceptionCheck()) return nullptr;
  }
  env->SetObjectArrayElement(result, 0, ranges);
  env->SetObjectArrayElement(result, 1, metadata);
  return result;
}

void throw_java(JNIEnv* env, std::exception const& failure)
{
  if (!env->ExceptionCheck()) {
    auto type = env->FindClass("java/lang/IllegalArgumentException");
    if (type) env->ThrowNew(type, failure.what());
  }
}

}  // namespace

// Do not inherit a dependency's JNI_OnLoad through dynamic symbol lookup. This host-only
// bridge has no cuDF Java-class registration or CUDA initialization requirements.
extern "C" JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM*, void*)
{
  return JNI_VERSION_1_8;
}

extern "C" JNIEXPORT jbyteArray JNICALL
Java_org_apache_spark_sql_cacheexploration_HostPackedBridge_create(
  JNIEnv* env, jclass, jint rows, jintArray java_types, jintArray java_nulls,
  jlongArray java_data, jlongArray java_masks, jlongArray java_offsets, jbyteArray java_packed)
{
  try {
    auto types = read_array<jint>(env, java_types);
    auto nulls = read_array<jint>(env, java_nulls);
    auto data = read_array<jlong>(env, java_data);
    auto masks = read_array<jlong>(env, java_masks);
    auto offsets = read_array<jlong>(env, java_offsets);
    auto columns = types.size();
    require(columns > 0 && rows > 0, "Host bridge requires nonempty columns and rows");
    require(nulls.size() == columns && data.size() == columns && masks.size() == columns &&
              offsets.size() == columns, "Descriptor array lengths differ");
    std::vector<std::uint8_t> metadata;
    {
      pinned_bytes packed(env, java_packed);
      std::vector<cudf::column_view> views;
      views.reserve(columns);
      for (std::size_t i = 0; i < columns; ++i) {
        check_column(types[i], rows, nulls[i], data[i], masks[i], offsets[i],
                     packed.data, packed.length);
        std::vector<cudf::column_view> children;
        if (types[i] == 23) {
          children.emplace_back(cudf::data_type(cudf::type_id::INT32), rows + 1,
                                packed.data + offsets[i], nullptr, 0);
        }
        views.emplace_back(cudf::data_type(static_cast<cudf::type_id>(types[i])), rows,
                           data[i] == -1 ? nullptr : packed.data + data[i],
                           masks[i] == -1 ? nullptr :
                             reinterpret_cast<cudf::bitmask_type const*>(packed.data + masks[i]),
                           nulls[i], 0, children);
      }
      // Views borrow host pointers. pack_metadata only records known sizes/counts/offsets;
      // no GPU split, data copy, allocation or null-count kernel is performed.
      metadata = cudf::pack_metadata(cudf::table_view(views), packed.data, packed.length);
    }
    require(metadata.size() <= static_cast<std::size_t>(std::numeric_limits<jsize>::max()),
            "Metadata exceeds Java array length");
    auto result = env->NewByteArray(static_cast<jsize>(metadata.size()));
    if (result) {
      env->SetByteArrayRegion(result, 0, static_cast<jsize>(metadata.size()),
                              reinterpret_cast<jbyte const*>(metadata.data()));
    }
    return result;
  } catch (std::exception const& failure) {
    throw_java(env, failure);
    return nullptr;
  }
}

extern "C" JNIEXPORT jlongArray JNICALL
Java_org_apache_spark_sql_cacheexploration_HostPackedBridge_parse(
  JNIEnv* env, jclass, jbyteArray java_metadata, jbyteArray java_packed)
{
  try {
    require(java_metadata != nullptr, "Null packed metadata");
    auto length = env->GetArrayLength(java_metadata);
    require(length > 0, "Empty packed metadata");
    std::vector<std::uint8_t> metadata(static_cast<std::size_t>(length));
    env->GetByteArrayRegion(java_metadata, 0, length, reinterpret_cast<jbyte*>(metadata.data()));
    if (env->ExceptionCheck()) return nullptr;
    std::vector<jlong> descriptors;
    {
      pinned_bytes packed(env, java_packed);
      auto view = cudf::unpack(std::span<std::uint8_t const>(metadata), packed.data);
      require(view.num_columns() > 0 && view.num_rows() > 0, "Empty packed table metadata");
      require(static_cast<std::uint64_t>(view.num_columns()) * 6 <=
                std::numeric_limits<jsize>::max(), "Too many packed columns");
      descriptors.reserve(static_cast<std::size_t>(view.num_columns()) * 6);
      for (auto const& col : view) {
        auto type = static_cast<int>(col.type().id());
        auto data = pointer_offset(col.head<void>(), packed.data, packed.length);
        auto mask = pointer_offset(col.null_mask(), packed.data, packed.length);
        jlong offsets = -1;
        if (type == 23) {
          require(col.num_children() == 1, "Expected one string offsets child");
          auto const& child = col.child(0);
          require(child.type().id() == cudf::type_id::INT32 &&
                    static_cast<std::int64_t>(child.size()) == static_cast<std::int64_t>(col.size()) + 1 &&
                    child.null_count() == 0 && child.offset() == 0 && child.num_children() == 0,
                  "Invalid string offsets child");
          offsets = pointer_offset(child.head<void>(), packed.data, packed.length);
        } else {
          require(col.num_children() == 0, "Fixed-width columns must have no children");
        }
        require(col.offset() == 0, "Packed column unexpectedly has nonzero row offset");
        check_column(type, col.size(), col.null_count(), data, mask, offsets,
                     packed.data, packed.length);
        descriptors.insert(descriptors.end(), {type, col.size(), col.null_count(), data, mask, offsets});
      }
    }
    auto result = env->NewLongArray(static_cast<jsize>(descriptors.size()));
    if (result) {
      env->SetLongArrayRegion(result, 0, static_cast<jsize>(descriptors.size()), descriptors.data());
    }
    return result;
  } catch (std::exception const& failure) {
    throw_java(env, failure);
    return nullptr;
  }
}

extern "C" JNIEXPORT jobjectArray JNICALL
Java_org_apache_spark_sql_cacheexploration_HostPackedBridge_indexDevice(
  JNIEnv* env, jclass, jbyteArray java_metadata, jlong address, jlong length)
{
  try {
    require(address != 0 && length > 0, "Invalid device packed allocation");
    auto base = static_cast<std::uintptr_t>(address);
    require(static_cast<std::uint64_t>(length) <=
              std::numeric_limits<std::uintptr_t>::max() - base, "Packed address overflow");
    auto metadata = read_metadata(env, java_metadata);
    auto pointer = reinterpret_cast<std::uint8_t const*>(base);
    auto table = cudf::unpack(std::span<std::uint8_t const>(metadata), pointer);
    return index_array(env, index_columns(table, pointer, static_cast<std::size_t>(length), false));
  } catch (std::exception const& failure) {
    throw_java(env, failure);
    return nullptr;
  }
}

extern "C" JNIEXPORT jobjectArray JNICALL
Java_org_apache_spark_sql_cacheexploration_HostPackedBridge_indexHost(
  JNIEnv* env, jclass, jbyteArray java_metadata, jbyteArray java_packed)
{
  try {
    auto metadata = read_metadata(env, java_metadata);
    column_index index;
    {
      pinned_bytes packed(env, java_packed);
      auto table = cudf::unpack(std::span<std::uint8_t const>(metadata), packed.data);
      index = index_columns(table, packed.data, packed.length, true);
    }
    // Allocate Java results only after releasing the borrowed critical packed array.
    return index_array(env, index);
  } catch (std::exception const& failure) {
    throw_java(env, failure);
    return nullptr;
  }
}

extern "C" JNIEXPORT jbyteArray JNICALL
Java_org_apache_spark_sql_cacheexploration_HostPackedBridge_project(
  JNIEnv* env, jclass, jobjectArray java_metadata, jlongArray java_lengths)
{
  try {
    require(java_metadata != nullptr, "Null column metadata array");
    auto lengths = read_array<jlong>(env, java_lengths);
    require(!lengths.empty() &&
              lengths.size() == static_cast<std::size_t>(env->GetArrayLength(java_metadata)),
            "Projected metadata/length counts differ or are empty");
    // Synthetic addresses are never dereferenced. pack_metadata only records offsets.
    constexpr std::uintptr_t synthetic_base = 0x100000000ULL;
    std::uint64_t total = 0;
    for (auto length : lengths) {
      require(length > 0 && length % 8 == 0, "Invalid projected column span length");
      require(static_cast<std::uint64_t>(length) <=
                static_cast<std::uint64_t>(std::numeric_limits<jint>::max()) - total,
              "Projected packed size exceeds Java array bound");
      total += length;
    }
    require(total <= std::numeric_limits<std::uintptr_t>::max() - synthetic_base,
            "Projected address overflow");
    std::vector<cudf::column_view> columns;
    columns.reserve(lengths.size());
    std::uint64_t position = 0;
    cudf::size_type rows = 0;
    for (std::size_t i = 0; i < lengths.size(); ++i) {
      auto array = static_cast<jbyteArray>(
        env->GetObjectArrayElement(java_metadata, static_cast<jsize>(i)));
      if (env->ExceptionCheck()) return nullptr;
      auto metadata = read_metadata(env, array);
      env->DeleteLocalRef(array);
      auto base = reinterpret_cast<std::uint8_t const*>(synthetic_base + position);
      auto table = cudf::unpack(std::span<std::uint8_t const>(metadata), base);
      require(table.num_columns() == 1, "Projection expects one-column metadata");
      auto const& col = table.column(0);
      check_span(col, base, static_cast<std::size_t>(lengths[i]), false);
      require(i == 0 || col.size() == rows, "Projected columns have different row counts");
      rows = col.size();
      columns.push_back(col);
      position += lengths[i];
    }
    auto metadata = cudf::pack_metadata(cudf::table_view(columns),
      reinterpret_cast<std::uint8_t const*>(synthetic_base), static_cast<std::size_t>(total));
    return metadata_array(env, metadata);
  } catch (std::exception const& failure) {
    throw_java(env, failure);
    return nullptr;
  }
}
