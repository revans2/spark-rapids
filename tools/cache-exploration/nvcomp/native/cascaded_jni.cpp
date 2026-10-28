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
#include <cuda_runtime_api.h>
#include <nvcomp/cascaded.h>
#include <dlfcn.h>
#include <algorithm>
#include <cstdint>
#include <exception>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#ifndef CASCADED_SDK_LIBRARY
#error CASCADED_SDK_LIBRARY must name the exact nvCOMP shared object
#endif

namespace {
static_assert(sizeof(size_t) == 8, "The cuDF batched framing uses 64-bit sizes");
constexpr size_t alignment = 8;
constexpr size_t max_batch = 1 << 20;

void require(bool valid, const char* message) {
  if (!valid) throw std::runtime_error(message);
}
size_t add(size_t a, size_t b) {
  require(a <= std::numeric_limits<size_t>::max() - b, "Workspace size overflow");
  return a + b;
}
size_t multiply(size_t a, size_t b) {
  require(b == 0 || a <= std::numeric_limits<size_t>::max() / b, "Workspace size overflow");
  return a * b;
}
size_t aligned(size_t n) { return add(n, alignment - 1) & ~(alignment - 1); }
size_t count(jlong n) {
  require(n > 0 && static_cast<uint64_t>(n) <= max_batch, "Invalid chunk count (maximum1048576)");
  return static_cast<size_t>(n);
}
void cuda_check(cudaError_t result, const char* stage) {
  if (result != cudaSuccess) throw std::runtime_error(std::string(stage) + ": " + cudaGetErrorString(result));
}
void nv_check(nvcompStatus_t result, const char* stage) {
  if (result != nvcompSuccess) throw std::runtime_error(std::string(stage) + ": nvCOMP status " + std::to_string(result));
}
nvcompBatchedCascadedCompressOpts_t options(int bits) {
  auto result = nvcompBatchedCascadedCompressDefaultOpts;
  switch (bits) {
    case 8: result.type = NVCOMP_TYPE_UCHAR; break;
    case 32: result.type = NVCOMP_TYPE_INT; break;
    case 64: result.type = NVCOMP_TYPE_LONGLONG; break;
    default: throw std::runtime_error("Invalid Cascaded integer width");
  }
  return result;
}
struct Api {
  void* handle;
  decltype(&nvcompBatchedCascadedCompressGetMaxOutputChunkSize) max_output;
  decltype(&nvcompBatchedCascadedCompressGetTempSizeAsync) compression_temp;
  decltype(&nvcompBatchedCascadedCompressAsync) compress;
  decltype(&nvcompBatchedCascadedDecompressGetTempSizeAsync) decompression_temp;
  decltype(&nvcompBatchedCascadedDecompressAsync) decompress;
  std::string bindings;

  template <typename T> T symbol(const char* name) {
    dlerror();
    void* address = dlsym(handle, name);
    const char* error = dlerror();
    if (error || !address) throw std::runtime_error(std::string("Missing SDK symbol ") + name);
    Dl_info info{};
    require(dladdr(address, &info) != 0 && info.dli_fname, "Cannot identify SDK symbol binding");
    bindings += std::string(name) + "=" + info.dli_fname + "\n";
    return reinterpret_cast<T>(address);
  }
  Api() {
    handle = dlopen(CASCADED_SDK_LIBRARY, RTLD_NOW | RTLD_LOCAL);
    if (!handle) throw std::runtime_error(std::string("Cannot load explicit SDK: ") + dlerror());
    try {
      max_output = symbol<decltype(max_output)>("nvcompBatchedCascadedCompressGetMaxOutputChunkSize");
      compression_temp = symbol<decltype(compression_temp)>("nvcompBatchedCascadedCompressGetTempSizeAsync");
      compress = symbol<decltype(compress)>("nvcompBatchedCascadedCompressAsync");
      decompression_temp = symbol<decltype(decompression_temp)>("nvcompBatchedCascadedDecompressGetTempSizeAsync");
      decompress = symbol<decltype(decompress)>("nvcompBatchedCascadedDecompressAsync");
    } catch (...) {
      dlclose(handle);
      throw;
    }
  }
  // Deliberately retained for process lifetime; CUDA work must not outlive SDK code.
};
Api& api() { static Api instance; return instance; }
size_t max_output(size_t chunk, int bits) {
  require(chunk > 0 && chunk <= nvcompCascadedCompressionMaxAllowedChunkSize && chunk % 8 == 0,
      "Invalid aligned Cascaded chunk size");
  size_t bytes = 0;
  nv_check(api().max_output(chunk, options(bits), &bytes), "Cascaded maximum output size");
  return aligned(bytes);
}
size_t status_bytes(size_t batch) { return aligned(multiply(batch, sizeof(nvcompStatus_t))); }
size_t compression_workspace(size_t batch, size_t chunk, size_t total, int bits) {
  size_t scratch = 0;
  nv_check(api().compression_temp(batch, chunk, options(bits), &scratch, total), "Cascaded compression scratch");
  return add(status_bytes(batch), aligned(scratch));
}
size_t compressed_stride(size_t chunk) {
  return std::max({max_output(chunk, 8), max_output(chunk, 32), max_output(chunk, 64)});
}
size_t decompression_workspace(size_t batch, size_t chunk, size_t total) {
  size_t scratch = 0;
  nv_check(api().decompression_temp(batch, chunk, nvcompBatchedCascadedDecompressDefaultOpts,
      &scratch, total), "Cascaded decompression scratch");
  // Status, actual output lengths, aligned input pointers, SDK scratch, and bounce chunks.
  return add(add(add(status_bytes(batch), multiply(batch, 16)), aligned(scratch)),
      multiply(batch, compressed_stride(chunk)));
}
std::vector<size_t> sizes(const size_t* device, size_t batch, cudaStream_t stream) {
  std::vector<size_t> result(batch);
  cuda_check(cudaMemcpyAsync(result.data(), device, multiply(batch, 8), cudaMemcpyDeviceToHost, stream), "Copy size vector");
  cuda_check(cudaStreamSynchronize(stream), "Synchronize size vector");
  return result;
}
std::vector<uintptr_t> addresses(const void* device, size_t batch, cudaStream_t stream) {
  return sizes(reinterpret_cast<const size_t*>(device), batch, stream);
}
void validate_status(nvcompStatus_t* device, size_t batch, cudaStream_t stream) {
  std::vector<nvcompStatus_t> status(batch);
  cuda_check(cudaMemcpyAsync(status.data(), device, multiply(batch, sizeof(nvcompStatus_t)), cudaMemcpyDeviceToHost, stream), "Copy Cascaded statuses");
  cuda_check(cudaStreamSynchronize(stream), "Synchronize Cascaded statuses");
  for (size_t i = 0; i < batch; ++i) {
    if (status[i] != nvcompSuccess) throw std::runtime_error("Cascaded chunk " + std::to_string(i) + " failed with status " + std::to_string(status[i]));
  }
}
struct SynchronizeOnFailure {
  cudaStream_t stream;
  int exceptions = std::uncaught_exceptions();
  ~SynchronizeOnFailure() {
    if (std::uncaught_exceptions() > exceptions) cudaStreamSynchronize(stream);
  }
};
void fail(JNIEnv* env, const std::exception& error) {
  env->ThrowNew(env->FindClass("java/lang/IllegalStateException"), error.what());
}
}

#define JNI_NAME(name) Java_cacheexploration_nvcomp_CascadedCodec_##name
extern "C" JNIEXPORT jlong JNICALL JNI_NAME(maxOutput)(JNIEnv* env, jclass, jlong chunk, jint bits) {
  try { return max_output(chunk, bits); } catch (const std::exception& e) { fail(env, e); return 0; }
}
extern "C" JNIEXPORT jlong JNICALL JNI_NAME(compressionWorkspace)(JNIEnv* env, jclass,
    jlong batch, jlong chunk, jlong total, jint bits) {
  try { require(total > 0, "Invalid total input size"); return compression_workspace(count(batch), chunk, total, bits); }
  catch (const std::exception& e) { fail(env, e); return 0; }
}
extern "C" JNIEXPORT jlong JNICALL JNI_NAME(decompressionWorkspace)(JNIEnv* env, jclass,
    jlong batch, jlong chunk, jlong total) {
  try { require(total > 0, "Invalid total output size"); return decompression_workspace(count(batch), chunk, total); }
  catch (const std::exception& e) { fail(env, e); return 0; }
}
extern "C" JNIEXPORT jstring JNICALL JNI_NAME(provenance)(JNIEnv* env, jclass) {
  try { return env->NewStringUTF(api().bindings.c_str()); }
  catch (const std::exception& e) { fail(env, e); return nullptr; }
}

extern "C" JNIEXPORT void JNICALL JNI_NAME(compressNative)(JNIEnv* env, jclass,
    jlong in, jlong in_sizes, jlong chunk, jlong num, jlong temp, jlong temp_size,
    jlong out, jlong out_sizes, jlong raw_stream, jint bits) {
  try {
    size_t batch = count(num);
    auto stream = reinterpret_cast<cudaStream_t>(raw_stream);
    auto inputs = addresses(reinterpret_cast<void*>(in), batch, stream);
    auto outputs = addresses(reinterpret_cast<void*>(out), batch, stream);
    auto lengths = sizes(reinterpret_cast<size_t*>(in_sizes), batch, stream);
    SynchronizeOnFailure cleanup{stream};
    size_t total = 0;
    for (size_t i = 0; i < batch; ++i) {
      require(inputs[i] % 8 == 0 && outputs[i] % 8 == 0, "Misaligned Cascaded compression pointer");
      require(lengths[i] > 0 && lengths[i] <= static_cast<size_t>(chunk) && lengths[i] % (bits / 8) == 0,
          "Cascaded chunk length violates integer width");
      total = add(total, lengths[i]);
    }
    require(temp % 8 == 0 && temp_size >= 0 && static_cast<size_t>(temp_size) >= compression_workspace(batch, chunk, total, bits), "Compression workspace too small");
    auto status = reinterpret_cast<nvcompStatus_t*>(temp);
    auto scratch = reinterpret_cast<void*>(temp + status_bytes(batch));
    size_t scratch_bytes = temp_size - status_bytes(batch);
    cuda_check(cudaMemsetAsync(status, 0xff, multiply(batch, sizeof(nvcompStatus_t)), stream), "Initialize compression statuses");
    nv_check(api().compress(reinterpret_cast<const void* const*>(in), reinterpret_cast<const size_t*>(in_sizes),
        chunk, batch, scratch, scratch_bytes, reinterpret_cast<void* const*>(out),
        reinterpret_cast<size_t*>(out_sizes), options(bits), status, stream), "Launch Cascaded compression");
    validate_status(status, batch, stream);
    auto actual = sizes(reinterpret_cast<size_t*>(out_sizes), batch, stream);
    size_t limit = max_output(chunk, bits);
    for (size_t n : actual) require(n > 0 && n <= limit, "Cascaded returned invalid compressed size");
  } catch (const std::exception& e) { fail(env, e); }
}

extern "C" JNIEXPORT void JNICALL JNI_NAME(decompressNative)(JNIEnv* env, jclass,
    jlong in, jlong in_sizes, jlong out_sizes, jlong num, jlong temp, jlong temp_size,
    jlong out, jlong raw_stream) {
  try {
    size_t batch = count(num);
    auto stream = reinterpret_cast<cudaStream_t>(raw_stream);
    auto inputs = addresses(reinterpret_cast<void*>(in), batch, stream);
    auto outputs = addresses(reinterpret_cast<void*>(out), batch, stream);
    auto lengths = sizes(reinterpret_cast<size_t*>(in_sizes), batch, stream);
    auto expected = sizes(reinterpret_cast<size_t*>(out_sizes), batch, stream);
    SynchronizeOnFailure cleanup{stream};
    size_t chunk = *std::max_element(expected.begin(), expected.end());
    // A final short chunk can be the only chunk; round up solely for conservative scratch sizing.
    chunk = aligned(chunk);
    size_t total = 0;
    for (size_t n : expected) total = add(total, n);
    require(temp % 8 == 0 && temp_size >= 0 && static_cast<size_t>(temp_size) >= decompression_workspace(batch, chunk, total), "Decompression workspace too small");
    auto status = reinterpret_cast<nvcompStatus_t*>(temp);
    auto actual = reinterpret_cast<size_t*>(temp + status_bytes(batch));
    auto device_inputs = reinterpret_cast<uintptr_t*>(actual + batch);
    auto scratch = reinterpret_cast<char*>(device_inputs + batch);
    size_t sdk_scratch = 0;
    nv_check(api().decompression_temp(batch, chunk, nvcompBatchedCascadedDecompressDefaultOpts,
        &sdk_scratch, total), "Cascaded decompression scratch");
    auto bounce = scratch + aligned(sdk_scratch);
    size_t stride = compressed_stride(chunk);
    for (size_t i = 0; i < batch; ++i) {
      require(expected[i] > 0 && outputs[i] % 8 == 0, "Invalid Cascaded output pointer or size");
      require(lengths[i] > 0 && lengths[i] <= stride, "Invalid Cascaded compressed size");
      if (inputs[i] % 8 != 0) {
        auto dest = bounce + multiply(i, stride);
        cuda_check(cudaMemcpyAsync(dest, reinterpret_cast<void*>(inputs[i]), lengths[i], cudaMemcpyDeviceToDevice, stream), "Align Cascaded input chunk");
        inputs[i] = reinterpret_cast<uintptr_t>(dest);
      }
    }
    cuda_check(cudaMemcpyAsync(device_inputs, inputs.data(), multiply(batch, 8), cudaMemcpyHostToDevice, stream), "Upload aligned input pointers");
    cuda_check(cudaMemsetAsync(status, 0xff, multiply(batch, sizeof(nvcompStatus_t)), stream), "Initialize decompression statuses");
    cuda_check(cudaMemsetAsync(actual, 0, multiply(batch, 8), stream), "Initialize actual decompressed sizes");
    nv_check(api().decompress(reinterpret_cast<const void* const*>(device_inputs), reinterpret_cast<const size_t*>(in_sizes),
        reinterpret_cast<const size_t*>(out_sizes), actual, batch, scratch, sdk_scratch,
        reinterpret_cast<void* const*>(out), nvcompBatchedCascadedDecompressDefaultOpts, status, stream), "Launch Cascaded decompression");
    validate_status(status, batch, stream);
    require(sizes(actual, batch, stream) == expected, "Cascaded decompressed lengths do not match requested sizes");
  } catch (const std::exception& e) { fail(env, e); }
}
