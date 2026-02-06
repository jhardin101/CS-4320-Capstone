from pathlib import Path
import sqlite3
import chess.pgn
import pandas as pd
from sklearn.model_selection import train_test_split

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "LumbrasGigaBase_OTB_2025.pgn"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def load_games(pgn_path):
    rows = []
    with open(pgn_path) as f:
        game_id = 0
        while True:
            game = chess.pgn.read_game(f)
            if game is None:
                break
        
            result = game.headers.get("Result")
            if result not in {"1-0", "0-1", "1/2-1/2"}:
                continue

            rows.append({
                "game_id": game_id,
                "result": result
            })
            game_id += 1

    print("Loaded metadata.")
    return pd.DataFrame(rows)

#80/10/10 split for train / validation / test
def split_sets(game_ids_df):
    

    train_ids, temp_ids = train_test_split(game_ids_df, test_size = 0.2, random_state = 42, shuffle = True)
    val_ids, test_ids = train_test_split(temp_ids, test_size = 0.5, random_state = 42, shuffle = True)

    print("Finished splitting ids.")

    return (set(train_ids), set(val_ids), set(test_ids))

def extract_pos(pgn_path, train_ids, val_ids, test_ids, k=4):
    train_rows, val_rows, test_rows  = [], [], []
    with open(pgn_path) as f:
        game_id = 0
        while True:
            game = chess.pgn.read_game(f)
            if game is None:
                break
            if game_id not in train_ids and game_id not in val_ids and game_id not in test_ids:
                game_id += 1
                continue

            result = game.headers.get("Result")
            if result not in {"1-0", "0-1", "1/2-1/2"}:
                game_id += 1
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
    return (pd.DataFrame(train_rows), pd.DataFrame(val_rows), pd.DataFrame(test_rows))

    
    
    return pd.DataFrame(rows)





def main():
    game_ids_df = load_games(DB_PATH)
    train_ids, val_ids, test_ids = split_sets(game_ids_df)

    train_df, val_df, test_df = extract_pos(DB_PATH, train_ids, val_ids, test_ids)

    print("done")


if __name__ == "__main__":
    main()