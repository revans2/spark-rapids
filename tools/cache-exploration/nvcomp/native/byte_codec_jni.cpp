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
#include <nvcomp/ans.h>
#include <nvcomp/snappy.h>
#include <dlfcn.h>
#include <algorithm>
#include <cstdint>
#include <exception>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

#ifndef CASCADED_SDK_LIBRARY
#error CASCADED_SDK_LIBRARY must name the exact nvCOMP shared object
#endif

namespace {
static_assert(sizeof(size_t) == 8, "The cuDF batched framing uses 64-bit sizes");
constexpr size_t alignment = 8;
constexpr size_t max_batch = 1 << 20;
constexpr size_t max_chunk = 1 << 24;

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
  require(n > 0 && static_cast<uint64_t>(n) <= max_batch, "Invalid chunk count");
  return static_cast<size_t>(n);
}
void check_chunk(size_t chunk) {
  require(chunk > 0 && chunk <= max_chunk && chunk % alignment == 0, "Invalid codec chunk size");
}
void cuda_check(cudaError_t result, const char* stage) {
  if (result != cudaSuccess) {
    throw std::runtime_error(std::string(stage) + ": " + cudaGetErrorString(result));
  }
}
void nv_check(nvcompStatus_t result, const char* stage) {
  if (result != nvcompSuccess) {
    throw std::runtime_error(std::string(stage) + ": nvCOMP status " + std::to_string(result));
  }
}

struct Snappy {
  using CompressOpts = nvcompBatchedSnappyCompressOpts_t;
  using DecompressOpts = nvcompBatchedSnappyDecompressOpts_t;
  static const char* name() { return "Snappy"; }
  static CompressOpts compress_opts() { return nvcompBatchedSnappyCompressDefaultOpts; }
  static DecompressOpts decompress_opts() { return nvcompBatchedSnappyDecompressDefaultOpts; }
};
struct Ans {
  using CompressOpts = nvcompBatchedANSCompressOpts_t;
  using DecompressOpts = nvcompBatchedANSDecompressOpts_t;
  static const char* name() { return "ANS"; }
  // Default rANS with generic one-byte data, not floating-point-specialized ANS.
  static CompressOpts compress_opts() { return nvcompBatchedANSCompressDefaultOpts; }
  static DecompressOpts decompress_opts() { return nvcompBatchedANSDecompressDefaultOpts; }
};

template <typename Codec> struct Api {
  using CodecType = Codec;
  using C = typename Codec::CompressOpts;
  using D = typename Codec::DecompressOpts;
  void* handle;
  nvcompStatus_t (*max_output)(size_t, C, size_t*);
  nvcompStatus_t (*compression_temp)(size_t, size_t, C, size_t*, size_t);
  nvcompStatus_t (*compress)(const void* const*, const size_t*, size_t, size_t,
      void*, size_t, void* const*, size_t*, C, nvcompStatus_t*, cudaStream_t);
  nvcompStatus_t (*decompression_temp)(size_t, size_t, D, size_t*, size_t);
  nvcompStatus_t (*decompress)(const void* const*, const size_t*, const size_t*, size_t*,
      size_t, void*, size_t, void* const*, D, nvcompStatus_t*, cudaStream_t);
  nvcompStatus_t (*compression_alignments)(C, nvcompAlignmentRequirements_t*);
  nvcompStatus_t (*decompression_alignments)(D, nvcompAlignmentRequirements_t*);
  nvcompAlignmentRequirements_t compression_alignment{};
  nvcompAlignmentRequirements_t decompression_alignment{};
  std::string bindings;

  template <typename T> T symbol(const char* suffix) {
    std::string name = std::string("nvcompBatched") + Codec::name() + suffix;
    dlerror();
    void* address = dlsym(handle, name.c_str());
    const char* error = dlerror();
    if (error || !address) throw std::runtime_error("Missing SDK symbol " + name);
    Dl_info info{};
    require(dladdr(address, &info) != 0 && info.dli_fname, "Cannot identify SDK symbol binding");
    bindings += name + "=" + info.dli_fname + "\n";
    return reinterpret_cast<T>(address);
  }
  static void validate_alignment(nvcompAlignmentRequirements_t a) {
    for (size_t n : {a.input, a.output, a.temp}) {
      require(n > 0 && n <= alignment && (alignment % n) == 0,
          "Codec requires alignment not supported by the cuDF framing adapter");
    }
  }
  Api() {
    handle = dlopen(CASCADED_SDK_LIBRARY, RTLD_NOW | RTLD_LOCAL);
    if (!handle) throw std::runtime_error(std::string("Cannot load explicit SDK: ") + dlerror());
    try {
      max_output = symbol<decltype(max_output)>("CompressGetMaxOutputChunkSize");
      compression_temp = symbol<decltype(compression_temp)>("CompressGetTempSizeAsync");
      compress = symbol<decltype(compress)>("CompressAsync");
      decompression_temp = symbol<decltype(decompression_temp)>("DecompressGetTempSizeAsync");
      decompress = symbol<decltype(decompress)>("DecompressAsync");
      compression_alignments = symbol<decltype(compression_alignments)>("CompressGetRequiredAlignments");
      decompression_alignments = symbol<decltype(decompression_alignments)>("DecompressGetRequiredAlignments");
      nv_check(compression_alignments(Codec::compress_opts(), &compression_alignment), "Compression alignment query");
      nv_check(decompression_alignments(Codec::decompress_opts(), &decompression_alignment), "Decompression alignment query");
      validate_alignment(compression_alignment);
      validate_alignment(decompression_alignment);
      bindings += std::string("options=") + Codec::name() + " default generic bytes\n";
    } catch (...) {
      dlclose(handle);
      throw;
    }
  }
  // Retained for process lifetime; outstanding CUDA work must not outlive SDK code.
};
template <typename Codec> Api<Codec>& api() { static Api<Codec> instance; return instance; }

template <typename F> auto dispatch(int codec, F&& operation) {
  switch (codec) {
    case 1: return operation(api<Snappy>());
    case 2: return operation(api<Ans>());
    default: throw std::runtime_error("Unknown byte codec");
  }
}

template <typename Codec> size_t max_output(Api<Codec>& sdk, size_t chunk) {
  check_chunk(chunk);
  size_t bytes = 0;
  nv_check(sdk.max_output(chunk, Codec::compress_opts(), &bytes), "Maximum compressed chunk size");
  return aligned(bytes);
}
size_t status_bytes(size_t batch) { return aligned(multiply(batch, sizeof(nvcompStatus_t))); }

template <typename Codec> size_t compression_workspace(
    Api<Codec>& sdk, size_t batch, size_t chunk, size_t total) {
  check_chunk(chunk);
  size_t scratch = 0;
  nv_check(sdk.compression_temp(batch, chunk, Codec::compress_opts(), &scratch, total),
      "Compression scratch query");
  return add(status_bytes(batch), aligned(scratch));
}
template <typename Codec> size_t decompression_workspace(
    Api<Codec>& sdk, size_t batch, size_t chunk, size_t total) {
  check_chunk(chunk);
  size_t scratch = 0;
  nv_check(sdk.decompression_temp(batch, chunk, Codec::decompress_opts(), &scratch, total),
      "Decompression scratch query");
  size_t bytes = add(add(status_bytes(batch), multiply(batch, 8)), aligned(scratch));
  // cuDF concatenates compressed chunks without padding. ANS may need aligned input copies.
  // Snappy permits byte alignment and uses the original input pointer vector directly.
  if (sdk.decompression_alignment.input > 1) {
    bytes = add(bytes, add(multiply(batch, 8), multiply(batch, max_output(sdk, chunk))));
  }
  return bytes;
}
struct SynchronizeOnFailure {
  cudaStream_t stream;
  int exceptions = std::uncaught_exceptions();
  ~SynchronizeOnFailure() {
    if (std::uncaught_exceptions() > exceptions) cudaStreamSynchronize(stream);
  }
};
std::vector<size_t> sizes(const size_t* device, size_t batch, cudaStream_t stream) {
  std::vector<size_t> result(batch);
  SynchronizeOnFailure cleanup{stream};
  cuda_check(cudaMemcpyAsync(result.data(), device, multiply(batch, 8),
      cudaMemcpyDeviceToHost, stream), "Copy chunk descriptors");
  cuda_check(cudaStreamSynchronize(stream), "Synchronize chunk descriptors");
  return result;
}
std::vector<uintptr_t> addresses(const void* device, size_t batch, cudaStream_t stream) {
  return sizes(reinterpret_cast<const size_t*>(device), batch, stream);
}
void validate_status(nvcompStatus_t* device, size_t batch, cudaStream_t stream) {
  std::vector<nvcompStatus_t> status(batch);
  SynchronizeOnFailure cleanup{stream};
  cuda_check(cudaMemcpyAsync(status.data(), device, multiply(batch, sizeof(nvcompStatus_t)),
      cudaMemcpyDeviceToHost, stream), "Copy codec statuses");
  cuda_check(cudaStreamSynchronize(stream), "Synchronize codec statuses");
  for (size_t i = 0; i < batch; ++i) {
    if (status[i] != nvcompSuccess) {
      throw std::runtime_error("Codec chunk " + std::to_string(i) + " failed with status "
          + std::to_string(status[i]));
    }
  }
}
void fail(JNIEnv* env, const std::exception& error) {
  env->ThrowNew(env->FindClass("java/lang/IllegalStateException"), error.what());
}
}

#define JNI_NAME(name) Java_cacheexploration_nvcomp_ByteCodec_##name

extern "C" JNIEXPORT jlong JNICALL JNI_NAME(maxOutput)(
    JNIEnv* env, jclass, jlong chunk, jint codec) {
  try { return dispatch(codec, [&](auto& sdk) { return max_output(sdk, chunk); }); }
  catch (const std::exception& e) { fail(env, e); return 0; }
}
extern "C" JNIEXPORT jlong JNICALL JNI_NAME(compressionWorkspace)(
    JNIEnv* env, jclass, jlong batch, jlong chunk, jlong total, jint codec) {
  try {
    require(total > 0, "Invalid total input size");
    return dispatch(codec, [&](auto& sdk) {
      return compression_workspace(sdk, count(batch), chunk, total);
    });
  } catch (const std::exception& e) { fail(env, e); return 0; }
}
extern "C" JNIEXPORT jlong JNICALL JNI_NAME(decompressionWorkspace)(
    JNIEnv* env, jclass, jlong batch, jlong chunk, jlong total, jint codec) {
  try {
    require(total > 0, "Invalid total output size");
    return dispatch(codec, [&](auto& sdk) {
      return decompression_workspace(sdk, count(batch), chunk, total);
    });
  } catch (const std::exception& e) { fail(env, e); return 0; }
}
extern "C" JNIEXPORT jstring JNICALL JNI_NAME(provenance)(JNIEnv* env, jclass, jint codec) {
  try {
    return dispatch(codec, [&](auto& sdk) { return env->NewStringUTF(sdk.bindings.c_str()); });
  } catch (const std::exception& e) { fail(env, e); return nullptr; }
}

extern "C" JNIEXPORT void JNICALL JNI_NAME(compressNative)(
    JNIEnv* env, jclass, jlong in, jlong in_sizes, jlong chunk, jlong num,
    jlong temp, jlong temp_size, jlong out, jlong out_sizes, jlong raw_stream, jint codec) {
  try {
    dispatch(codec, [&](auto& sdk) {
      using Codec = typename std::decay_t<decltype(sdk)>::CodecType;
      size_t batch = count(num);
      auto stream = reinterpret_cast<cudaStream_t>(raw_stream);
      auto inputs = addresses(reinterpret_cast<void*>(in), batch, stream);
      auto outputs = addresses(reinterpret_cast<void*>(out), batch, stream);
      auto lengths = sizes(reinterpret_cast<size_t*>(in_sizes), batch, stream);
      SynchronizeOnFailure cleanup{stream};
      size_t total = 0;
      check_chunk(chunk);
      for (size_t i = 0; i < batch; ++i) {
        require(inputs[i] % sdk.compression_alignment.input == 0
            && outputs[i] % sdk.compression_alignment.output == 0,
            "Misaligned codec compression pointer");
        require(lengths[i] > 0 && lengths[i] <= static_cast<size_t>(chunk),
            "Invalid uncompressed chunk length");
        total = add(total, lengths[i]);
      }
      require(temp % alignment == 0 && temp_size >= 0
          && static_cast<size_t>(temp_size) >= compression_workspace(sdk, batch, chunk, total),
          "Compression workspace too small");
      auto status = reinterpret_cast<nvcompStatus_t*>(temp);
      auto scratch = reinterpret_cast<void*>(temp + status_bytes(batch));
      size_t scratch_bytes = temp_size - status_bytes(batch);
      cuda_check(cudaMemsetAsync(status, 0xff, multiply(batch, sizeof(nvcompStatus_t)), stream),
          "Initialize compression statuses");
      nv_check(sdk.compress(reinterpret_cast<const void* const*>(in),
          reinterpret_cast<const size_t*>(in_sizes), chunk, batch, scratch, scratch_bytes,
          reinterpret_cast<void* const*>(out), reinterpret_cast<size_t*>(out_sizes),
          Codec::compress_opts(), status, stream), "Launch codec compression");
      validate_status(status, batch, stream);
      auto actual = sizes(reinterpret_cast<size_t*>(out_sizes), batch, stream);
      size_t limit = max_output(sdk, chunk);
      for (size_t n : actual) require(n > 0 && n <= limit, "Invalid compressed chunk size");
    });
  } catch (const std::exception& e) { fail(env, e); }
}

extern "C" JNIEXPORT void JNICALL JNI_NAME(decompressNative)(
    JNIEnv* env, jclass, jlong in, jlong in_sizes, jlong out_sizes, jlong num,
    jlong temp, jlong temp_size, jlong out, jlong raw_stream, jlong raw_chunk, jint codec) {
  try {
    dispatch(codec, [&](auto& sdk) {
      using Codec = typename std::decay_t<decltype(sdk)>::CodecType;
      size_t batch = count(num);
      size_t chunk = raw_chunk;
      check_chunk(chunk);
      auto stream = reinterpret_cast<cudaStream_t>(raw_stream);
      auto inputs = addresses(reinterpret_cast<void*>(in), batch, stream);
      auto outputs = addresses(reinterpret_cast<void*>(out), batch, stream);
      auto lengths = sizes(reinterpret_cast<size_t*>(in_sizes), batch, stream);
      auto expected = sizes(reinterpret_cast<size_t*>(out_sizes), batch, stream);
      SynchronizeOnFailure cleanup{stream};
      size_t total = 0;
      for (size_t n : expected) {
        require(n > 0 && n <= chunk, "Invalid requested decompressed chunk size");
        total = add(total, n);
      }
      require(temp % alignment == 0 && temp_size >= 0
          && static_cast<size_t>(temp_size) >= decompression_workspace(sdk, batch, chunk, total),
          "Decompression workspace too small");
      auto status = reinterpret_cast<nvcompStatus_t*>(temp);
      auto actual = reinterpret_cast<size_t*>(temp + status_bytes(batch));
      bool needs_alignment = sdk.decompression_alignment.input > 1;
      auto device_inputs = reinterpret_cast<uintptr_t*>(actual + batch);
      auto scratch = reinterpret_cast<char*>(device_inputs + (needs_alignment ? batch : 0));
      size_t sdk_scratch = 0;
      nv_check(sdk.decompression_temp(batch, chunk, Codec::decompress_opts(), &sdk_scratch, total),
          "Decompression scratch query");
      auto bounce = scratch + aligned(sdk_scratch);
      size_t stride = max_output(sdk, chunk);
      for (size_t i = 0; i < batch; ++i) {
        require(outputs[i] % sdk.decompression_alignment.output == 0,
            "Misaligned codec decompression output");
        require(lengths[i] > 0 && lengths[i] <= stride, "Invalid compressed chunk size");
        if (inputs[i] % sdk.decompression_alignment.input != 0) {
          auto dest = bounce + multiply(i, stride);
          cuda_check(cudaMemcpyAsync(dest, reinterpret_cast<void*>(inputs[i]), lengths[i],
              cudaMemcpyDeviceToDevice, stream), "Align compressed codec input");
          inputs[i] = reinterpret_cast<uintptr_t>(dest);
        }
      }
      const void* const* input_vector = reinterpret_cast<const void* const*>(in);
      if (needs_alignment) {
        cuda_check(cudaMemcpyAsync(device_inputs, inputs.data(), multiply(batch, 8),
            cudaMemcpyHostToDevice, stream), "Upload aligned input pointers");
        input_vector = reinterpret_cast<const void* const*>(device_inputs);
      }
      cuda_check(cudaMemsetAsync(status, 0xff, multiply(batch, sizeof(nvcompStatus_t)), stream),
          "Initialize decompression statuses");
      cuda_check(cudaMemsetAsync(actual, 0, multiply(batch, 8), stream),
          "Initialize decompressed chunk lengths");
      nv_check(sdk.decompress(input_vector, reinterpret_cast<const size_t*>(in_sizes),
          reinterpret_cast<const size_t*>(out_sizes), actual, batch, scratch, sdk_scratch,
          reinterpret_cast<void* const*>(out), Codec::decompress_opts(), status, stream),
          "Launch codec decompression");
      validate_status(status, batch, stream);
      require(sizes(actual, batch, stream) == expected, "Decompressed chunk lengths mismatch");
    });
  } catch (const std::exception& e) { fail(env, e); }
}
