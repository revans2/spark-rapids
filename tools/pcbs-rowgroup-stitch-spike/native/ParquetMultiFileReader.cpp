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
#include "cudf_jni_apis.hpp"
#include "jni_utils.hpp"

#include <cudf/io/parquet.hpp>
#include <cudf/utilities/span.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

extern "C" JNIEXPORT jlongArray JNICALL
Java_ai_rapids_cudf_ParquetMultiFileReader_00024Native_readFiles(
  JNIEnv* env, jclass, jobjectArray columns, jint timestamp_type, jlongArray addresses_and_lengths)
{
  JNI_NULL_CHECK(env, addresses_and_lengths, "file buffers are required", nullptr);
  JNI_NULL_CHECK(env, columns, "column selection is required", nullptr);
  JNI_TRY
  {
    cudf::jni::auto_set_device(env);
    cudf::jni::native_jlongArray addresses(env, addresses_and_lengths);
    cudf::jni::native_jstringArray selected_columns(env, columns);
    CUDF_EXPECTS(addresses.size() > 0 && addresses.size() % 2 == 0,
                 "Expected nonempty address/length pairs");
    std::vector<cudf::host_span<std::byte const>> files;
    files.reserve(addresses.size() / 2);
    for (std::size_t i = 0; i < addresses.size(); i += 2) {
      CUDF_EXPECTS(addresses[i] != 0 && addresses[i + 1] > 0,
                   "Each independent file must be nonempty");
      files.emplace_back(reinterpret_cast<std::byte const*>(addresses[i]),
                         static_cast<std::size_t>(addresses[i + 1]));
    }
    // Unlike Table's HMB varargs overload, each span is a separate source/file.
    cudf::io::source_info source{
      cudf::host_span<cudf::host_span<std::byte const>>{files.data(), files.size()}};
    auto builder = cudf::io::parquet_reader_options::builder(source);
    if (selected_columns.size() > 0) {
      builder.column_names(selected_columns.as_cpp_vector());
    }
    auto options = builder.convert_strings_to_categories(false)
                     .timestamp_type(cudf::data_type(static_cast<cudf::type_id>(timestamp_type)))
                     .ignore_missing_columns(true)
                     .build();
    auto table = cudf::io::read_parquet(options).tbl;
    addresses.cancel();
    return cudf::jni::convert_table_for_return(env, table);
  }
  JNI_CATCH(env, nullptr);
}
