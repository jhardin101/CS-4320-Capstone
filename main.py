from pathlib import Path
import chess.pgn
import pandas as pd
from sklearn.model_selection import train_test_split
from tqdm import tqdm
import time
import os

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "LumbrasGigaBase_OTB_2025.pgn"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# Cache directories for dataframes
CACHE_DIR = BASE_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
TRAIN_PATH = CACHE_DIR / "train.parquet"
VAL_PATH = CACHE_DIR / "val.parquet"
TEST_PATH = CACHE_DIR / "test.parquet"

def cache_exists():
    return TRAIN_PATH.exists() and VAL_PATH.exists() and TEST_PATH.exists()

def load_cached():
    return (
        pd.read_parquet(TRAIN_PATH),
        pd.read_parquet(VAL_PATH),
        pd.read_parquet(TEST_PATH)
    )

def save_cache(train_df, val_df, test_df):
    train_df.to_parquet(TRAIN_PATH)
    val_df.to_parquet(VAL_PATH)
    test_df.to_parquet(TEST_PATH)

def load_or_create_datasets():
    if cache_exists():
        return load_cached()
    
    print("Preprocessing PGN...")
    games_df = load_games(DB_PATH)
    train_ids, val_ids, test_ids = split_sets(games_df)
    train_df, val_df, test_df = extract_pos(DB_PATH, train_ids, val_ids, test_ids)

    save_cache(train_df, val_df, test_df)
    return train_df, val_df, test_df

def load_games(pgn_path):
    rows = []
    start_time = time.time()
    with open(pgn_path) as f:
        game_id = 0
        with tqdm(desc="Loading games") as pbar:
            while True:
                game = chess.pgn.read_game(f)
                if game is None:
                    break
            
                result = game.headers.get("Result")
                if result not in {"1-0", "0-1", "1/2-1/2"}:
                    game_id += 1
                    pbar.update(1)
                    continue

                rows.append({
                    "game_id": game_id,
                    "result": result
                })
                game_id += 1
                pbar.update(1)

    elapsed = time.time() - start_time
    print(f"Loaded {len(rows)} games in {elapsed:.2f}s")
    return pd.DataFrame(rows)

#80/10/10 split for train / validation / test
def split_sets(game_ids_df):
    

    train_ids, temp_ids = train_test_split(game_ids_df, test_size = 0.2, random_state = 42, shuffle = True)
    val_ids, test_ids = train_test_split(temp_ids, test_size = 0.5, random_state = 42, shuffle = True)

    print("Finished splitting ids.")

    return (set(train_ids['game_id']), set(val_ids['game_id']), set(test_ids['game_id']))

def extract_pos(pgn_path, train_ids, val_ids, test_ids, k=4):
    train_rows, val_rows, test_rows  = [], [], []
    start_time = time.time()
    with open(pgn_path) as f:
        game_id = 0
        with tqdm(desc="Extracting positions") as pbar:
            while True:
                game = chess.pgn.read_game(f)
                if game is None:
                    break
                if game_id not in train_ids and game_id not in val_ids and game_id not in test_ids:
                    game_id += 1
                    pbar.update(1)
                    continue

                result = game.headers.get("Result")
                if result not in {"1-0", "0-1", "1/2-1/2"}:
                    game_id += 1
                    pbar.update(1)
                    continue

                board = game.board()
                for ply, move in enumerate(game.mainline_moves()):
                    board.push(move)
                    if ply % k == 0:
                        row = {
                            "game_id": game_id,
                            "fen": board.fen(),
                            "result": result,
                            "ply": ply
                        }
                        if game_id in train_ids:
                            train_rows.append(row)
                        elif game_id in val_ids:
                            val_rows.append(row)
                        else:
                            test_rows.append(row)
                game_id += 1
                pbar.update(1)
    
    elapsed = time.time() - start_time
    print(f"Extracted positions in {elapsed:.2f}s")
    print(f"  Train: {len(train_rows)} positions")
    print(f"  Val:   {len(val_rows)} positions")
    print(f"  Test:  {len(test_rows)} positions")
    return (pd.DataFrame(train_rows), pd.DataFrame(val_rows), pd.DataFrame(test_rows))



def main():
    start_time = time.time()
    train_df, val_df, test_df = load_or_create_datasets()

    total_elapsed = time.time() - start_time
    print(f"\nCompleted in {total_elapsed:.2f}s total")



if __name__ == "__main__":
    main()