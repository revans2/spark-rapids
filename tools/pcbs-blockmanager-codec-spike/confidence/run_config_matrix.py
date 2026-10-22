#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Run independent config confidence cases using generated frozen-fixture copies."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--spark-home", required=True, type=Path)
parser.add_argument("--output-dir", required=True, type=Path)
args = parser.parse_args()
source = Path(__file__).resolve().parents[1]
output = args.output_dir.resolve()
output.mkdir(parents=True, exist_ok=False)
sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
cases = {
    "kryo": {"spark.serializer": "org.apache.spark.serializer.KryoSerializer"},
    "compression": {"spark.rdd.compress": "true"},
    "encryption": {"spark.io.encryption.enabled": "true"},
}
results = {}
for name, settings in cases.items():
    generated = output / (name + "-sources")
    generated.mkdir()
    files = list(source.glob("*.scala")) + [source / "run_spike.py", source / "prepare_overlay.py"]
    for path in files:
        shutil.copyfile(path, generated / path.name)
    original_hashes = {path.name: sha(path) for path in files}
    runner = generated / "run_spike.py"
    text = runner.read_text()
    anchor = "for k, v in conf.items():"
    assert text.count(anchor) == 1
    text = text.replace(anchor, "conf.update(" + repr(settings) + ")\n" + anchor)
    runner.write_text(text)

    # Instrument the generated source overlay only. Record the object actually
    # returned by the existing stream wrappers, rather than just configured flags.
    prepare = generated / "prepare_overlay.py"
    text = prepare.read_text()
    anchor = "    target = args.output_dir / Path(relative).name"
    injected = """    if relative.endswith("SerializerManager.scala"):
        for stream_type, direction in (("InputStream", "input"), ("OutputStream", "output")):
            old = ("  def wrapStream(blockId: BlockId, s: " + stream_type + "): " +
                   stream_type + " = {\\n" +
                   "    wrapForCompression(blockId, wrapForEncryption(s))\\n  }")
            new = ("  def wrapStream(blockId: BlockId, s: " + stream_type + "): " +
                   stream_type + " = {\\n" +
                   "    val wrapped = wrapForCompression(blockId, wrapForEncryption(s))\\n" +
                   "    println(\\"CONFIG_MATRIX STREAM direction=" + direction +
                   " block=\\" + blockId + \\" encryption=\\" + encryptionEnabled +" +
                   " \\" compression=\\" + shouldCompress(blockId) +" +
                   " \\" class=\\" + wrapped.getClass.getName)\\n" +
                   "    wrapped\\n  }")
            assert modified.count(old) == 1
            modified = modified.replace(old, new)
"""
    injected += """    if relative.endswith("SerializerManager.scala"):
        for direction in ("Input", "Output"):
            old = ("if (shouldCompress(blockId)) compressionCodec.compressed" +
                   direction + "Stream(s) else s")
            new = ("{ val wrapped = " + old + "; println(" +
                   '\\"CONFIG_MATRIX STREAM direction=' + direction.lower() +
                   ' block=\\" + blockId + \\" compression=\\" + shouldCompress(blockId)' +
                   ' + \\" class=\\" + wrapped.getClass.getName); wrapped }')
            assert modified.count(old) == 1
            modified = modified.replace(old, new)
            old = "CryptoStreamUtils.createCrypto" + direction + "Stream(s, conf, key)"
            new = ("{ val wrapped = " + old + "; println(" +
                   '\\"CONFIG_MATRIX CRYPTO direction=' + direction.lower() +
                   ' class=\\" + wrapped.getClass.getName); wrapped }')
            assert modified.count(old) == 1
            modified = modified.replace(old, new)
"""
    assert text.count(anchor) == 1
    prepare.write_text(text.replace(anchor, injected + anchor))

    fixture = generated / "BlockManagerCodecSpike.scala"
    text = fixture.read_text()
    check = {
        "kryo": ('require(SparkEnv.get.serializer.isInstanceOf[KryoSerializer])'),
        "compression": ('require(sc.getConf.getBoolean("spark.rdd.compress", false))'),
        "encryption": ('require(SparkEnv.get.serializerManager.encryptionEnabled)'),
    }[name]
    anchor = '    sc.setLogLevel("WARN")'
    assert text.count(anchor) == 1
    fixture.write_text(text.replace(anchor, anchor + "\n    " + check +
                                   '\n    println("CONFIG_MATRIX EFFECTIVE ' + name + '")'))
    storage = generated / "StorageRouteChecks.scala"
    text = storage.read_text()
    # Inspecting getUnrolledChunkedByteBuffer closes the still-live byte sink.
    # Count a nonempty consumed prefix and nonempty remainder without closing it.
    text = text.replace("        def input = Iterator.range(0, 32).map(i =>",
                        "        var pulled = 0\n        def input = " +
                        "Iterator.range(0, 32).map(i => { pulled += 1; i }).map(i =>")
    text = text.replace(
        'require(partial.getUnrolledChunkedByteBuffer.size > 0, "no serialized prefix")',
        'require(pulled > 0 && pulled < 32, "expected a serialized prefix and remainder")')
    storage.write_text(text)
    if name == "compression":
        # Repeated values compress below the admission threshold. Use deterministic
        # incompressible records so the original partial-unroll criterion still fires.
        anchor_mix = "    val capacity = env.memoryManager.maxOnHeapStorageMemory"
        mix = """    def mixedLong(value: Long): Long = {
      var z = value + 0x9e3779b97f4a7c15L
      z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L
      z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL
      z ^ (z >>> 31)
    }
"""
        assert text.count(anchor_mix) == 1
        text = text.replace(anchor_mix, mix + anchor_mix)
        text = text.replace("Array.fill(32768)(i.toLong)",
                            "Array.tabulate(32768)(j => mixedLong(i * 32768L + j))")
        text = text.replace("batch.values.forall(_ == i.toLong)",
                            "batch.values.indices.forall(j => " +
                            "batch.values(j) == mixedLong(i * 32768L + j))")
        storage.write_text(text)
    anchor = "    val restored = manager.getLocalValues(block).get"
    if name != "kryo":
        check = ('require(raw.length < original.length * 8, "compression was not applied")'
                 if name == "compression" else
                 'require(java.nio.ByteBuffer.wrap(raw).getInt() != 0x1abc0001, '
                 '"raw disk file exposes plaintext codec framing")')
        snippet = """    val raw = java.nio.file.Files.readAllBytes(manager.diskBlockManager.getFile(block).toPath)
    """ + check + """
    println("CONFIG_MATRIX RAW_DISK bytes=" + raw.length + " logicalBytes=" + original.length * 8)
"""
        if name == "encryption":
            snippet += """    val encrypted = manager.diskStore.getBytes(block)
    require(encrypted.isInstanceOf[EncryptedBlockData])
    val decrypted = new java.io.DataInputStream(encrypted.toInputStream())
    try require(decrypted.readInt() == 0x1abc0001) finally decrypted.close()
    encrypted.dispose()
    println("CONFIG_MATRIX DECRYPTION class=EncryptedBlockData plaintextMagic=true")
"""
        assert text.count(anchor) == 1
        storage.write_text(text.replace(anchor, snippet + anchor))
    case_output = output / name
    argv = ["python", str(runner), "--spark-home", str(args.spark_home),
            "--output-dir", str(case_output)]
    subprocess_result = subprocess.run(argv, timeout=360)
    primary = json.loads((case_output / "result.json").read_text())
    raw = (case_output / "run.log").read_text()
    streams = [line for line in raw.splitlines() if line.startswith("CONFIG_MATRIX STREAM ")]
    effective = raw.count("CONFIG_MATRIX EFFECTIVE " + name + "\n") == 1
    specific = True
    if name in ("compression", "encryption"):
        if name == "compression":
            selected = [line for line in streams
                        if "block=rdd_900001_0 " in line and "compression=true" in line]
        else:
            selected = [line for line in raw.splitlines()
                        if line.startswith("CONFIG_MATRIX CRYPTO ") and "Crypto" in line]
        input_proof = (any("direction=input" in line for line in selected)
                       if name == "compression" else raw.count(
                           "CONFIG_MATRIX DECRYPTION class=EncryptedBlockData plaintextMagic=true") == 1)
        specific = (any("direction=output" in line for line in selected) and
                    input_proof and raw.count("CONFIG_MATRIX RAW_DISK ") == 1)
    valid = subprocess_result.returncode == 0 and primary["valid"] and effective and specific
    evidence = {
        "valid": valid, "settings": settings, "argv": argv,
        "original_sources": original_hashes,
        "generated_sources": {path.name: sha(path) for path in generated.iterdir()},
        "primary_result_sha256": sha(case_output / "result.json"),
        "manifest_sha256": sha(case_output / "manifest.json"),
        "effective_assertion": effective, "stream_path_assertion": specific,
        "stream_diagnostics": streams, "remaining_limits": [
            "small heap-backed mechanism test, not PCBS performance or native storage",
            "independent configurations, no combination matrix",
            "Spark 3.5.5 only"],
    }
    (case_output / "config-verification.json").write_text(json.dumps(evidence, indent=2) + "\n")
    results[name] = evidence
    if not valid:
        break
summary = {"valid": len(results) == len(cases) and all(item["valid"] for item in results.values()),
           "cases": {name: {"valid": value["valid"], "settings": value["settings"]}
                     for name, value in results.items()},
           "runner_sha256": sha(Path(__file__))}
(output / "config-matrix-result.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary))
raise SystemExit(0 if summary["valid"] else 1)
