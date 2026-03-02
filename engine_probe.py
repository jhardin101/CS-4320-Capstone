# engine_probe.py
import time
import math
import pandas as pd
from pathlib import Path
import chess, chess.engine
import random
import sys

# Adjust these for your environment
ENGINE_PATH = "stockfish"          # or full path to stockfish binary
ENGINE_TIME = 0.05                 # seconds per position used in probe
SAMPLE_SIZE = 200                  # 100-500 recommended
CACHE_PATH = Path("cache/engine_evals.parquet")

def load_fens_from_cache_or_parquet(parquet_path="cache/train.parquet", max_rows=5000):
    p = Path(parquet_path)
    if p.exists():
        df = pd.read_parquet(p)
        if "fen" in df.columns:
            return df["fen"].astype(str).tolist()
    # fallback: try to read any cached engine evals file to get fens
    if CACHE_PATH.exists():
        df = pd.read_parquet(CACHE_PATH)
        if "fen" in df.columns:
            return df["fen"].astype(str).tolist()
    raise FileNotFoundError("No parquet with 'fen' found. Run extractor or point parquet_path to a file with FENs.")

def _score_to_cp(score):
    if score.is_mate():
        mate = score.mate()
        return 100000 if mate > 0 else -100000
    return score.score()

def probe(engine_path=ENGINE_PATH, eval_time=ENGINE_TIME, sample_size=SAMPLE_SIZE):
    fens = load_fens_from_cache_or_parquet()
    random.seed(42)
    sample = random.sample(fens, min(sample_size, len(fens)))
    print(f"Probing {len(sample)} positions with engine_time={eval_time}s ...")

    # warmup engine once
    try:
        with chess.engine.SimpleEngine.popen_uci(engine_path) as eng:
            # optional warmup: analyse first position quickly
            _ = eng.analyse(chess.Board(sample[0]), chess.engine.Limit(time=min(0.01, eval_time)))
            t0 = time.time()
            for fen in sample:
                board = chess.Board(fen)
                info = eng.analyse(board, chess.engine.Limit(time=eval_time))
                _ = _score_to_cp(info["score"].white())
            t_total = time.time() - t0
    except Exception as e:
        print("Engine error:", e)
        sys.exit(1)

    avg = t_total / len(sample)
    print(f"Total probe time: {t_total:.2f}s")
    print(f"Average time per position: {avg:.4f}s")

    # Extrapolate common dataset sizes
    sizes = [10000, 50000, 100000]
    print("\nExtrapolated wall times (single engine, sequential):")
    for s in sizes:
        secs = s * avg
        hrs = secs / 3600.0
        print(f"  {s:,} positions -> {secs:.0f}s ({hrs:.2f} hours)")

    # Suggest parallelization factor
    cpus = 4
    print(f"\nEstimate with {cpus} parallel engines (ideal): { (sizes[0] * avg) / cpus / 3600.0 :.2f} hours for {sizes[0]:,} positions")

if __name__ == "__main__":
    probe()
