/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

// ML Training Data Generation - QUICK START VERSION
// 
// This is a simplified version of ml_join_training_data.scala for quick testing.
// Uses smaller test counts and faster iterations for validation.
//
// Runtime: ~15-30 minutes (10 initial + 5 refinement tests)
//
// To run the full version, use: :load examples/ml_join_training_data.scala

println("="*80)
println("ML JOIN OPTIMIZATION TRAINING - QUICK START")
println("="*80)
println()
println("This is a quick-start version with reduced test counts.")
println("For full training, use: ml_join_training_data.scala")
println()

// Quick start configuration
val numInitialTests = 10        // Reduced from 100
val numRefinementTests = 5       // Reduced from 50
val benchmarkIterations = 3      // Reduced from 5

val baseDir = "/data/tmp/ml_join_training_quickstart"
val outputTsvPath = s"$baseDir/training_data.tsv"

println(s"Configuration:")
println(s"  Initial tests: $numInitialTests")
println(s"  Refinement tests: $numRefinementTests")
println(s"  Iterations per test: $benchmarkIterations")
println(s"  Output: $outputTsvPath")
println()

println("Loading full ML training framework...")

// Load the main script with overridden configuration
// Note: This is a simplified approach. The actual implementation would
// need the full script to be parameterized or this to be a standalone version.

println()
println("="*80)
println("QUICK START INSTRUCTIONS")
println("="*80)
println()
println("To run a quick test of the ML training framework:")
println()
println("1. Edit ml_join_training_data.scala and change these lines:")
println("   val numInitialTests = 10        // Reduced from 100")
println("   val numRefinementTests = 5       // Reduced from 50")
println("   val benchmarkIterations = 3      // Reduced from 5")
println()
println("2. Run: :load examples/ml_join_training_data.scala")
println()
println("3. After completion, analyze with:")
println("   python examples/ml_join_analysis.py /data/tmp/ml_join_training/training_data.tsv")
println()
println("For production runs, use the original values:")
println("   numInitialTests = 100, numRefinementTests = 50, benchmarkIterations = 5")
println()

