from pathlib import Path
from multiprocessing import Pool, cpu_count
import chess.pgn
import chess
import chess.engine
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from sklearn.model_selection import train_test_split, GridSearchCV, KFold, validation_curve
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix
from tqdm import tqdm
import time
import os
import joblib
import seaborn as sns
import shutil, stat, sys


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "LumbrasGigaBase_OTB_2025.pgn"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# Cache directories for dataframes
CACHE_DIR = BASE_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

#games.parquet holds metadata
GAMES_PATH = CACHE_DIR / "games.parquet"
TRAIN_PATH = CACHE_DIR / "train.parquet"
VAL_PATH = CACHE_DIR / "val.parquet"
TEST_PATH = CACHE_DIR / "test.parquet"

# Configuration constants
ENGINE_PATH = "stockfish"            # or full path to your stockfish binary
ENGINE_TIME = 0.05                   # seconds per position
EVAL_CACHE = CACHE_DIR / "engine_evals.parquet"
MARGIN_CP = 50                       # centipawn margin for "clear" advantage
MODEL_OUT = CACHE_DIR / "chess_engine_classifier.joblib"
VALIDATION_CURVE_PNG = CACHE_DIR / "validation_curve.png"

# Classification Macros
MAX_ITERATIONS = 2000

def find_engine(engine_name_or_path="stockfish"):
    # prefer absolute path if provided
    path = engine_name_or_path if os.path.isabs(engine_name_or_path) else (shutil.which(engine_name_or_path) or engine_name_or_path)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Engine not found at '{path}'. Install Stockfish or set ENGINE_PATH to the binary location.")
    # ensure executable bit
    if not os.access(path, os.X_OK):
        try:
            st = os.stat(path)
            os.chmod(path, st.st_mode | stat.S_IXUSR)
        except PermissionError:
            raise PermissionError(f"Engine binary at '{path}' is not executable. Run: chmod +x {path} or install system-wide.")
    return path


def cache_exists():
    return TRAIN_PATH.exists() and VAL_PATH.exists() and TEST_PATH.exists() and GAMES_PATH.exists()

def load_cached():
    return (
        pd.read_parquet(GAMES_PATH),
        pd.read_parquet(TRAIN_PATH),
        pd.read_parquet(VAL_PATH),
        pd.read_parquet(TEST_PATH)
    )

def save_cache(train_df, val_df, test_df, games_df):
    games_df.to_parquet(GAMES_PATH)
    train_df.to_parquet(TRAIN_PATH)
    val_df.to_parquet(VAL_PATH)
    test_df.to_parquet(TEST_PATH)

def load_or_create_datasets():
    if cache_exists():
        games_df, train_df, val_df, test_df = load_cached()
        train_ids = set(train_df['game_id'])
        val_ids = set(val_df['game_id'])
        test_ids = set(test_df['game_id'])
        return games_df, train_df, val_df, test_df, train_ids, val_ids, test_ids
    
    print("Preprocessing PGN...")
    games_df = load_games(DB_PATH)
    train_ids, val_ids, test_ids = split_sets(games_df)
    train_df, val_df, test_df = extract_pos(DB_PATH, train_ids, val_ids, test_ids)

    save_cache(train_df, val_df, test_df, games_df)
    return games_df, train_df, val_df, test_df, train_ids, val_ids, test_ids

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
            
                headers = game.headers
                result = headers.get("Result")
                if result not in {"1-0", "0-1", "1/2-1/2"}:
                    game_id += 1
                    pbar.update(1)
                    continue

                white_elo = headers.get("WhiteElo")
                black_elo = headers.get("BlackElo")

                rows.append({
                    "game_id": game_id,
                    "result": result,
                    "white_elo": white_elo,
                    "black_elo": black_elo
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


# Assignment 5B Classification
def encode_labels(df):
    mapping = {"0-1":0, "1/2-1/2":1, "1-0":2}
    df = df.copy()
    df["label"] = df["result"].map(mapping)
    return df

def fen_to_features(fen):
    # parse fen string quickly: material counts, side-to-move, castling flags 
    board = chess.Board(fen)
    #material counts
    piece_map = board.piece_map()
    mat_white = 0
    mat_black = 0
    for piece in piece_map.values():
        val = {'P':1, 'N':3, 'B':3, 'R':5, 'Q':9, 'K':0}[piece.symbol().upper()]
        if piece.color:
            mat_white += val
        else:
            mat_black += val
    # return dict e.g. {"mat_diff":..., "side_to_move":..., "castle_w_k":..., "castle_b_q":...}
    return{
        'material_diff': mat_white - mat_black,
        'side_to_move_white': int(board.turn == chess.WHITE),
        'castle_K': int(board.has_kingside_castling_rights(chess.WHITE)),
        'castle_Q': int(board.has_queenside_castling_rights(chess.WHITE)),
        'castle_k': int(board.has_kingside_castling_rights(chess.BLACK)),
        'castle_q': int(board.has_queenside_castling_rights(chess.BLACK)),
        'en_passant_exists': int(board.ep_square is not None),
        'halfmove_clock': board.halfmove_clock,
        'fullmove_number': board.fullmove_number,
        'legal_moves_count': board.legal_moves.count()
    }


def prepare_features(df):
    rows = []
    for fen, ply in tqdm(zip(df["fen"].values, df["ply"].values),
                         total=len(df), desc="Extracting features"):
        rows.append(fen_to_features(fen))
    X = pd.DataFrame(rows)
    X["ply"] = df["ply"].values
    return X


def make_logistic(max_iter=2000):

    try:
        # modern API
        return LogisticRegression(multi_class='multinomial', solver='saga',
                                  max_iter=max_iter, class_weight='balanced')
    except TypeError:
        # older sklearn
        try:
            return LogisticRegression(solver='lbfgs', max_iter=max_iter, class_weight='balanced')
        except TypeError:
            # very old sklearn: use defaults
            return LogisticRegression(max_iter=max_iter)
        
#Additions in 6B

#Engine evaluation helpers
def _score_to_cp(score):
    """Convert chess.engine.Score to centipawn integer from White's perspective."""
    if score.is_mate():
        mate = score.mate()
        return 100000 if mate > 0 else -100000
    return score.score()

def eval_fen_with_engine_cached(fen, engine, eval_cache):
    """Evaluate a single FEN using an open engine instance and an in-memory cache dict."""
    if fen in eval_cache:
        return eval_cache[fen]
    board = chess.Board(fen)
    info = engine.analyse(board, chess.engine.Limit(time=ENGINE_TIME))
    cp = _score_to_cp(info["score"].white())
    eval_cache[fen] = cp
    return cp

def load_eval_cache():
    if EVAL_CACHE.exists():
        try:
            df = pd.read_parquet(EVAL_CACHE)
            return dict(zip(df["fen"].values, df["eval_cp"].values))
        except Exception:
            return {}
    return {}

def save_eval_cache(eval_cache):
    df = pd.DataFrame({"fen": list(eval_cache.keys()), "eval_cp": list(eval_cache.values())})
    df.to_parquet(EVAL_CACHE)


def add_engine_labels_df(df, engine_path=ENGINE_PATH, eval_time=ENGINE_TIME, margin=MARGIN_CP, cache=None,
                         parallel_workers=None, chunk_size=2000, save_every_chunks=5):
    df = df.copy().reset_index(drop=True)
    if cache is None:
        cache = load_eval_cache()
    # ensure engine exists (but allow fallback to cache-only)
    try:
        engine_bin = find_engine(engine_path)
    except Exception as e:
        print("Engine not available:", e)
        print("Using cached evaluations only (rows without cached evals will be dropped).")
        df["eval_cp"] = df["fen"].map(cache)
        df = df[df["eval_cp"].notna()].reset_index(drop=True)
        df = df[df["eval_cp"].abs() > margin].reset_index(drop=True)
        df["label_engine"] = (df["eval_cp"] > 0).astype(int)
        return df

    # build list of fens to evaluate (only missing ones)
    fens = df["fen"].astype(str).tolist()
    # run parallel evaluator to fill cache for missing fens
    updated_cache = parallel_evaluate_fens(fens, engine_bin, eval_time=eval_time,
                                          n_workers=parallel_workers, chunk_size=chunk_size,
                                          save_every_chunks=save_every_chunks)
    # attach and filter
    df["eval_cp"] = df["fen"].map(updated_cache)
    df = df[df["eval_cp"].notna()].reset_index(drop=True)
    df = df[df["eval_cp"].abs() > margin].reset_index(drop=True)
    df["label_engine"] = (df["eval_cp"] > 0).astype(int)
    return df



def _score_to_cp(score):
    if score.is_mate():
        mate = score.mate()
        return 100000 if mate > 0 else -100000
    return score.score()

def _worker_eval_chunk(args):
    fen_chunk, engine_path, eval_time = args
    out = {}
    try:
        with chess.engine.SimpleEngine.popen_uci(engine_path) as eng:
            for fen in fen_chunk:
                try:
                    info = eng.analyse(chess.Board(fen), chess.engine.Limit(time=eval_time))
                    out[fen] = _score_to_cp(info["score"].white())
                except Exception:
                    out[fen] = 0
    except Exception:
        for fen in fen_chunk:
            out[fen] = 0
    return out

def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def parallel_evaluate_fens(fen_list, engine_path, eval_time=ENGINE_TIME,
                           n_workers=None, chunk_size=2000, save_every_chunks=5):
    if n_workers is None:
        n_workers = max(1, min(cpu_count() - 1, 4))

    existing = load_eval_cache()

    # Full chunking for global progress
    full_chunks = list(chunk_list(fen_list, chunk_size))
    total_chunks = len(full_chunks)

    # Determine missing FENs
    to_eval = [f for f in fen_list if f not in existing]

    # Chunk only missing FENs
    missing_chunks = list(chunk_list(to_eval, chunk_size))
    missing_total = len(missing_chunks)

    # Chunks already satisfied by cache
    already_done = total_chunks - missing_total

    # If nothing to evaluate, return immediately
    if missing_total == 0:
        return existing

    args = [(chunk, engine_path, eval_time) for chunk in missing_chunks]
    merged = dict(existing)

    try:
        with Pool(processes=n_workers) as pool:
            with tqdm(total=total_chunks,
                      initial=already_done,
                      desc="Engine eval chunks") as pbar:

                for i, result in enumerate(pool.imap_unordered(_worker_eval_chunk, args), 1):
                    merged.update(result)
                    pbar.update(1)

                    if i % save_every_chunks == 0:
                        save_eval_cache(merged)

    except KeyboardInterrupt:
        save_eval_cache(merged)
        raise

    save_eval_cache(merged)
    return merged

def main():
    start_time = time.time()
    games_df, train_df, val_df, test_df, train_ids, val_ids, test_ids = load_or_create_datasets()

    # choose workers and sample strategy here; None uses auto-detected workers
    WORKERS = None
    CHUNK_SIZE = 2000
    SAVE_EVERY_CHUNKS = 5

    print("Annotating train set with engine evaluations (may take time on first run)...")
    train_df = add_engine_labels_df(train_df, parallel_workers=WORKERS, chunk_size=CHUNK_SIZE, save_every_chunks=SAVE_EVERY_CHUNKS)
    print("Annotating val set with engine evaluations...")
    val_df = add_engine_labels_df(val_df, parallel_workers=WORKERS, chunk_size=CHUNK_SIZE, save_every_chunks=SAVE_EVERY_CHUNKS)
    print("Annotating test set with engine evaluations...")
    test_df = add_engine_labels_df(test_df, parallel_workers=WORKERS, chunk_size=CHUNK_SIZE, save_every_chunks=SAVE_EVERY_CHUNKS)


    print(f"After margin filter: Train={len(train_df)}, Val={len(val_df)}, Test={len(test_df)}")

    # Prepare features
    X_train = prepare_features(train_df)
    y_train = train_df["label_engine"].values
    X_val = prepare_features(val_df)
    y_val = val_df["label_engine"].values
    X_test = prepare_features(test_df)
    y_test = test_df["label_engine"].values

    # Pipeline 
    pipeline = Pipeline([
        ('imputer', SimpleImputer(strategy='mean')),
        ('scaler', StandardScaler()),
        ('clf', LogisticRegression(solver='saga', max_iter=MAX_ITERATIONS, class_weight='balanced', random_state=42))
    ])

    # Grid search for regularization strength 
    param_grid = {"clf__C": [0.01, 0.1, 1, 2, 5, 10, 50, 100]}
    cv = KFold(n_splits=5, shuffle=True, random_state=42)
    grid = GridSearchCV(pipeline, param_grid, scoring="f1", cv=cv, n_jobs=-1, return_train_score=True)
    print("Running GridSearchCV...")
    grid.fit(X_train, y_train)

    print("Best params:", grid.best_params_)
    print("Best CV F1:", grid.best_score_)

    # Validation curve plot (train vs val mean F1)
    C_range = np.array(param_grid["clf__C"])
    train_scores = grid.cv_results_["mean_train_score"]
    val_scores = grid.cv_results_["mean_test_score"]
    # map scores to C order 
    plt.figure(figsize=(8,5))
    sns.set_style("whitegrid")
    plt.semilogx(C_range, train_scores, marker='o', label="Train F1 (mean)")
    plt.semilogx(C_range, val_scores, marker='o', label="CV F1 (mean)")
    plt.xlabel("C (log scale)")
    plt.ylabel("F1 score")
    plt.title("Validation Curve for clf__C")
    plt.legend()
    plt.tight_layout()
    plt.savefig(VALIDATION_CURVE_PNG, dpi=200)
    plt.close()

    # Evaluate best estimator on test set
    best_model = grid.best_estimator_
    y_test_pred = best_model.predict(X_test)
    print("Test classification report:")
    print(classification_report(y_test, y_test_pred, digits=4))
    print("Confusion matrix:")
    print(confusion_matrix(y_test, y_test_pred))

    # Save final model
    joblib.dump(best_model, MODEL_OUT)
    elapsed = time.time() - start_time
    print(f"Training + eval completed in {elapsed:.2f}s. Model saved to {MODEL_OUT}")

    return grid, best_model


if __name__ == "__main__":
    main()