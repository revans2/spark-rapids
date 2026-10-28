# Legacy Cascaded JNI adapter

This standalone experiment uses installed nvCOMP5.0.0.6 Cascaded, not the redesigned
V2 codec or a CPU codec. It changes no production cuDF or RAPIDS interfaces.

Build without GPU execution:

```
python tools/cache-exploration/nvcomp/native/build.py --output /data/tmp/cascaded-build
```

The output directory must be new. The build records commands and SHA256 for the
SDK library, headers, cuDF runtime jar, adapter sources and generated JNI library.
`CascadedCodec.nativeProvenance()` reports the `dladdr` shared-object location of
every Cascaded function resolved with `dlsym` on the explicitly loaded SDK handle.
The bridge links CUDA runtime and libdl, not an unqualified `-lnvcomp` search.

After reserving the GPU, launch `cacheexploration.nvcomp.CascadedSmoke` with the
compiled classes, exact cuDF runtime jar and its SLF4J dependency on the classpath.
Pass `-ea` and
`-Dcacheexploration.cascaded.library=/absolute/path/libcascaded_exploration.so`.
The smoke prints actual chunk lengths and pointer misalignment counts before
round trips. It covers signed32/64-bit values, arbitrary byte patterns, odd byte
tails, multichunk inputs and rejected width-invalid input. It checks live RMM
allocation bytes return to zero. This is a correctness prerequisite, not a timed
benchmark. Do not launch it concurrently with another GPU job.

## Ownership and framing

The factories return subclasses of the installed cuDF BatchedCompressor and
BatchedDecompressor. Their four-long scratch callbacks match runtime26.12, which
differs from the mounted older cuDF Java sources. Compression consumes input
references; decompression consumes compressed input references and borrows output
buffers. Factory chunks must be positive eight-byte multiples, at most16MiB.
Inputs must be nonempty, eight-byte aligned and multiples of the chosen width.
The byte codec uses UCHAR,32-bit uses INT,64-bit uses LONGLONG; these are the exact
SDK enum constants. The SDK defaults supply two RLE passes, one delta pass and
bitpacking. No CPU compression/decompression is provided.

The inherited framing stores eight-byte compressed lengths then concatenates
payloads without padding. A compressed length need not preserve the next chunk's
SDK-required eight-byte alignment. The decoder preserves this framing and copies
misaligned compressed chunks into aligned RMM workspace before a single batched
SDK call. It never serializes chunk decompression. The constructor's maximum
compressed stride is rounded up to eight bytes so compression output addresses
also remain aligned.

## Allocation and timing accounting

No native cudaMalloc allocations occur. The superclass allocates scratch through
DeviceMemoryBuffer/RMM. Scratch includes per-chunk statuses, actual decoded sizes,
aligned input pointers, SDK scratch and conservative batch bounce capacity:

- Compression: align8(statusBytes) + align8(SDKscratch).
- Decompression: align8(statusBytes) +16*chunkCount + align8(SDKscratch) +
  chunkCount*maxAlignedCompressedStride.

Bounce capacity is reserved for every chunk, even when only some or none need a
copy. Report this reserved scratch separately from actual copy bytes and retained
payload. Host descriptor/status vectors are ordinary bounded native allocations;
chunk count is capped at1,048,576. They are not RMM host allocations.

Both callbacks synchronize to validate statuses before returning. Compression also
checks positive actual compressed lengths against the SDK bound. Decompression
compares every actual output length with the caller's expected length. Additional
host descriptor copies and synchronization are included in callback timing; these
are not asynchronous codec-only latency measurements. An exception guard drains
queued stream work before borrowed host descriptors or Java-owned scratch can be
released. No corruption-resilience claim is made for arbitrary untrusted framing;
the superclass requires exact output sizes and valid corresponding chunk metadata.
