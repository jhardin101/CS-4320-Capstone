from pathlib import Path
from multiprocessing import Pool, cpu_count
import chess.pgn
import chess
import chess.engine
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from sklearn.model_selection import train_test_split, GridSearchCV, KFold, validation_curve, StratifiedShuffleSplit
from sklearn.pipeline import Pipeline
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, HistGradientBoostingClassifier
from sklearn.preprocessing import FunctionTransformer, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix, silhouette_score, precision_score, recall_score, f1_score, roc_auc_score
from sklearn.svm import SVC
from tqdm import tqdm
import time
import os
import joblib
import seaborn as sns
import shutil, stat, sys
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.calibration import CalibratedClassifierCV
from sklearn.pipeline import make_pipeline
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset



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
RANDOM_STATE = 42

# choose workers and sample strategy here; None uses auto-detected workers
WORKERS = None
CHUNK_SIZE = 2000
SAVE_EVERY_CHUNKS = 5

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
    

    train_ids, temp_ids = train_test_split(game_ids_df, test_size = 0.2, random_state = RANDOM_STATE, shuffle = True)
    val_ids, test_ids = train_test_split(temp_ids, test_size = 0.5, random_state = RANDOM_STATE, shuffle = True)

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

def log_regress():
    start_time = time.time()
    games_df, train_df, val_df, test_df, train_ids, val_ids, test_ids = load_or_create_datasets()

    

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
        ('clf', LogisticRegression(solver='saga', max_iter=MAX_ITERATIONS, class_weight='balanced', random_state=RANDOM_STATE))
    ])

    # Grid search for regularization strength 
    param_grid = {"clf__C": [0.01, 0.1, 1, 2, 5, 10, 50, 100]}
    cv = KFold(n_splits=5, shuffle=True, random_state=RANDOM_STATE)
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

def compare_nb_knn_logistic(X_train, y_train, X_val, y_val, X_test, y_test):
    results = {}

    # Common preprocessing pipeline: imputer + scaler
    preproc = Pipeline([
        ('imputer', SimpleImputer(strategy='mean')),
        ('scaler', StandardScaler())
    ])

    X_train_p = preproc.fit_transform(X_train)
    X_val_p = preproc.transform(X_val)
    X_test_p = preproc.transform(X_test)

    # 1) Gaussian Naive Bayes
    gnb = GaussianNB()
    start = time.time()
    gnb.fit(X_train_p, y_train)
    train_time = time.time() - start
    start = time.time()
    y_pred = gnb.predict(X_test_p)
    pred_time = (time.time() - start) / len(X_test_p)
    results['GaussianNB'] = {'model': gnb, 'train_time': train_time, 'pred_time_per_sample': pred_time,
                             'report': classification_report(y_test, y_pred, output_dict=True)}

    # 2) KNN (grid over k)
    knn = KNeighborsClassifier()
    param_grid = {'n_neighbors': [3,5,9], 'weights': ['uniform','distance'], 'metric': ['euclidean','manhattan']}
    grid = GridSearchCV(knn, param_grid, scoring='f1', cv=3, n_jobs=-1)
    start = time.time()
    grid.fit(X_train_p, y_train)
    train_time = time.time() - start
    best_knn = grid.best_estimator_
    start = time.time()
    y_pred = best_knn.predict(X_test_p)
    pred_time = (time.time() - start) / len(X_test_p)
    results['KNN'] = {'model': best_knn, 'train_time': train_time, 'pred_time_per_sample': pred_time,
                      'best_params': grid.best_params_,
                      'report': classification_report(y_test, y_pred, output_dict=True)}

    # 3) Logistic 
    log = LogisticRegression(solver='saga', max_iter=MAX_ITERATIONS, class_weight='balanced', random_state=RANDOM_STATE)
    start = time.time()
    log.fit(X_train_p, y_train)
    train_time = time.time() - start
    start = time.time()
    y_pred = log.predict(X_test_p)
    pred_time = (time.time() - start) / len(X_test_p)
    results['Logistic'] = {'model': log, 'train_time': train_time, 'pred_time_per_sample': pred_time,
                           'report': classification_report(y_test, y_pred, output_dict=True)}

    return results

def stratified_sample(df, label_col='label_engine', n=10000, random_state=RANDOM_STATE):
    n = min(n, len(df))
    if n == len(df):
        return df.sample(frac=1, random_state=random_state).reset_index(drop=True)
    sss = StratifiedShuffleSplit(n_splits=1, train_size=n, random_state=random_state)
    X = [[i] for i in range(len(df))]
    y = df[label_col].values
    idx, _ =next(sss.split(X, y))
    return df.iloc[idx].reset_index(drop=True)

# Assignment 7 B
def model_fam_comparison():
    games_df, train_df, val_df, test_df, train_ids, val_ids, test_ids = load_or_create_datasets()

    

    print("Annotating train set with engine evaluations (may take time on first run)...")
    train_df = add_engine_labels_df(train_df, parallel_workers=WORKERS, chunk_size=CHUNK_SIZE, save_every_chunks=SAVE_EVERY_CHUNKS)
    print("Annotating val set with engine evaluations...")
    val_df = add_engine_labels_df(val_df, parallel_workers=WORKERS, chunk_size=CHUNK_SIZE, save_every_chunks=SAVE_EVERY_CHUNKS)
    print("Annotating test set with engine evaluations...")
    test_df = add_engine_labels_df(test_df, parallel_workers=WORKERS, chunk_size=CHUNK_SIZE, save_every_chunks=SAVE_EVERY_CHUNKS)


    print(f"After margin filter: Train={len(train_df)}, Val={len(val_df)}, Test={len(test_df)}")

    # Sample a small proxy dataset for testing
    train_df = stratified_sample(train_df)
    val_df = stratified_sample(val_df)
    test_df = stratified_sample(test_df)
    

    X_train = prepare_features(train_df)
    y_train = train_df["label_engine"].values
    X_val = prepare_features(val_df)
    y_val = val_df["label_engine"].values
    X_test = prepare_features(test_df)
    y_test = test_df["label_engine"].values

    print(compare_nb_knn_logistic(X_train, y_train, X_val, y_val, X_test, y_test))

# Assignment 8 B, SVMs
def svm_time():
    games_df, train_df, val_df, test_df, train_ids, val_ids, test_ids = load_or_create_datasets()
    print("Annotating train set with engine evaluations (may take time on first run)...")
    train_df = add_engine_labels_df(train_df, parallel_workers=WORKERS, chunk_size=CHUNK_SIZE, save_every_chunks=SAVE_EVERY_CHUNKS)
    print("Annotating val set with engine evaluations...")
    val_df = add_engine_labels_df(val_df, parallel_workers=WORKERS, chunk_size=CHUNK_SIZE, save_every_chunks=SAVE_EVERY_CHUNKS)
    print("Annotating test set with engine evaluations...")
    test_df = add_engine_labels_df(test_df, parallel_workers=WORKERS, chunk_size=CHUNK_SIZE, save_every_chunks=SAVE_EVERY_CHUNKS)

    print(f"After margin filter: Train={len(train_df)}, Val={len(val_df)}, Test={len(test_df)}")

    # Sample a small proxy dataset for testing
    train_df = stratified_sample(train_df)
    val_df = stratified_sample(val_df)
    test_df = stratified_sample(test_df)

    # Prepare features for training:
    X_train = prepare_features(train_df)
    y_train = train_df["label_engine"].values
    X_val = prepare_features(val_df)
    y_val = val_df["label_engine"].values
    X_test = prepare_features(test_df)
    y_test = test_df["label_engine"].values

    # Pipelines
    linear = Pipeline([
        ('imputer', SimpleImputer(strategy="mean")),
        ("scaler", StandardScaler()),
        ('clf', SVC(
            kernel="linear",
            C=1.0,
            class_weight='balanced',
            verbose=1
        ))
    ])

    rbf = Pipeline([
        ('imputer', SimpleImputer(strategy='mean')),
        ('scaler', StandardScaler()),
        ('clf', SVC(
            kernel='rbf',
            C=1.0,
            gamma=0.1,
            class_weight='balanced',
            verbose=1
        ))
    ])

    # Train models
    linear.fit(X_train, y_train)
    rbf.fit(X_train, y_train)

    scores_linear = linear.decision_function(X_val)
    scores_rbf = rbf.decision_function(X_val)

    linear_y_pred = (scores_linear >= 0).astype(int)
    rbf_y_pred = (scores_rbf >= 0).astype(int)\
    
    print("Linear SVM Report: ")
    print(classification_report(y_val, linear_y_pred, output_dict=True))

    print("RBF SVM Report: ")
    print(classification_report(y_val, rbf_y_pred, output_dict=True))

# Assignment 9 B: Ensembles
def fetch_dataframes():
    games_df, train_df, val_df, test_df, train_ids, val_ids, test_ids = load_or_create_datasets()
    print("Annotating train set with engine evaluations (may take time on first run)...")
    train_df = add_engine_labels_df(train_df, parallel_workers=WORKERS, chunk_size=CHUNK_SIZE, save_every_chunks=SAVE_EVERY_CHUNKS)
    print("Annotating val set with engine evaluations...")
    val_df = add_engine_labels_df(val_df, parallel_workers=WORKERS, chunk_size=CHUNK_SIZE, save_every_chunks=SAVE_EVERY_CHUNKS)
    print("Annotating test set with engine evaluations...")
    test_df = add_engine_labels_df(test_df, parallel_workers=WORKERS, chunk_size=CHUNK_SIZE, save_every_chunks=SAVE_EVERY_CHUNKS)

    print(f"After margin filter: Train={len(train_df)}, Val={len(val_df)}, Test={len(test_df)}")
    return train_df, val_df, test_df

def ensemble_tests():
    train_df, val_df, test_df = fetch_dataframes()

    # Sample a small proxy dataset for testing
    train_df = stratified_sample(train_df,n=50000)
    val_df = stratified_sample(val_df, n=50000)
    test_df = stratified_sample(test_df, n=50000)

    # Prepare features for training:
    X_train = prepare_features(train_df)
    y_train = train_df["label_engine"].values
    X_val = prepare_features(val_df)
    y_val = val_df["label_engine"].values
    X_test = prepare_features(test_df)
    y_test = test_df["label_engine"].values

    # Pipelines and Preprocessing
    tree_preprocess = Pipeline([
        ("imputer", SimpleImputer(strategy="median"))
    ])

    # Baseline Tree : high variance, instructive failures
    tree_pipeline = Pipeline([
        ("preprocess", tree_preprocess),
        ("clf", DecisionTreeClassifier(
            max_depth=None,
            min_samples_leaf=5,
            class_weight="balanced",
            random_state=RANDOM_STATE
        ))
    ])

    # Random Forest
    rf_pipeline = Pipeline([
        ("preprocess", tree_preprocess),
        ("clf", RandomForestClassifier(
            n_estimators=200,
            max_depth=None,
            min_samples_leaf=5,
            n_jobs=1,
            class_weight="balanced",
            random_state=RANDOM_STATE
        ))
    ])

    # Histogram-based Gradient Boosting
    hgb_pipeline = Pipeline([
        ("preprocess", tree_preprocess),
        ("clf", HistGradientBoostingClassifier(
            max_depth=6,
            learning_rate=0.1,
            max_iter=200,
            random_state=RANDOM_STATE
        ))
    ])

    # Evaluation
    models = {
        "Decision Tree": tree_pipeline,
        "Random Forest": rf_pipeline,
        "HistGradientBoosting": hgb_pipeline
    }

    for name, model in models.items():
        model.fit(X_train, y_train)
        y_pred = model.predict(X_val)
        print(f"\n{name}")
        print(classification_report(y_val, y_pred, digits=4))

# Assignment 10 Part B: Unsupervised Learning
def unsupervised_tests():
    # retrieve dataframes
    train_df, _, _ = fetch_dataframes()
    train_df = prepare_features(train_df)

    # Make proxy dataset for faster tests
    n = min(10000, len(train_df))
    train_df = train_df.sample(n=n, random_state=RANDOM_STATE).reset_index(drop=True)
    
    
    preprocess = Pipeline(steps=[ 
                ("imputer", SimpleImputer(strategy="median")), 
                ("scaler", StandardScaler()) 
                ])
    X_proc = preprocess.fit_transform(train_df)

    # Explained Variance
    pca_full = PCA()
    pca_full.fit(X_proc)

    explained_var = pca_full.explained_variance_ratio_
    c_var = np.cumsum(explained_var)

    for i in range(11):
        print(f"PC{i+1}: {explained_var[i]:.3f}, cumulative: {c_var[i]: .3f}")

    # 2D PCA Vis
    pca = PCA(n_components=2, random_state=RANDOM_STATE)
    X_2d = pca.fit_transform(X_proc)

    plt.figure(figsize=(7,6))
    plt.scatter(X_2d[:, 0], X_2d[:, 1], alpha=0.5)
    plt.xlabel("PC1")
    plt.ylabel("PC2")
    plt.title("PCA Projection (2D)")
    plt.savefig("pca.png", dpi=300, bbox_inches="tight")
    plt.show()
    
    # K Means
    results = []
    ks = [2,3,4,5,6,7,8,9,10]

    for k in ks:
        kmeans = KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init=10)
        labels = kmeans.fit_predict(X_proc)

        inertia = kmeans.inertia_
        sil = silhouette_score(X_proc, labels)

        results.append((k, inertia, sil))
        print(f"k={k}: inertia={inertia:.1f}, silhouette={sil:.3f}")
   
    # Final Model
    k_final = 2
    kmeans = KMeans(n_clusters=k_final, random_state=RANDOM_STATE, n_init=10)
    labels = kmeans.fit_predict(X_proc)

    X_labeled = train_df.copy()
    X_labeled["cluster"] = labels

    cluster_summary = X_labeled.groupby("cluster").mean(numeric_only=True)
    print(cluster_summary)

# Assignment 11 Part B: Neural Networks (MLP Proxy)

def fetch_dataframes():
    """Load cached or create train/val/test dataframes with engine labels."""
    games_df, train_df, val_df, test_df, train_ids, val_ids, test_ids = load_or_create_datasets()
    
    print("Annotating train set with engine evaluations...")
    train_df = add_engine_labels_df(train_df, parallel_workers=WORKERS, 
                                    chunk_size=CHUNK_SIZE, save_every_chunks=SAVE_EVERY_CHUNKS)
    print("Annotating val set with engine evaluations...")
    val_df = add_engine_labels_df(val_df, parallel_workers=WORKERS, 
                                  chunk_size=CHUNK_SIZE, save_every_chunks=SAVE_EVERY_CHUNKS)
    print("Annotating test set with engine evaluations...")
    test_df = add_engine_labels_df(test_df, parallel_workers=WORKERS, 
                                   chunk_size=CHUNK_SIZE, save_every_chunks=SAVE_EVERY_CHUNKS)
    
    print(f"After margin filter: Train={len(train_df)}, Val={len(val_df)}, Test={len(test_df)}")
    return train_df, val_df, test_df

def mlp_neural_proxy():
    """
    MLP proxy comparison against GBM baseline.
    
    Approach: Uses hand-engineered features (10 dims) to train an MLP
    and compares performance to existing HistGradientBoosting baseline (F1=0.7802).
    
    Research question: Does MLP add non-linear benefit over GBM for this task?
    """
    start_time = time.time()
    
    # Load datasets
    train_df, val_df, test_df = fetch_dataframes()
    
    # Prepare features (same hand-engineered features used for GBM)
    print("\nPreparing features...")
    X_train = prepare_features(train_df)
    y_train = train_df["label_engine"].values
    
    X_val = prepare_features(val_df)
    y_val = val_df["label_engine"].values
    
    X_test = prepare_features(test_df)
    y_test = test_df["label_engine"].values
    
    print(f"Feature shapes: X_train={X_train.shape}, X_val={X_val.shape}, X_test={X_test.shape}")
    print(f"Class distribution: y_train={np.bincount(y_train.astype(int))}")
    
    # === Baseline: HistGradientBoosting (from Assignment 6 Part B) ===
    print("\n" + "="*70)
    print("BASELINE: HistGradientBoosting Classifier")
    print("="*70)
    
    tree_preprocess = Pipeline([
        ('imputer', SimpleImputer(strategy='mean')),
        ('scaler', StandardScaler()),
    ])
    
    hgb_pipeline = Pipeline([
        ("preprocess", tree_preprocess),
        ("clf", HistGradientBoostingClassifier(
            max_depth=6,
            learning_rate=0.1,
            max_iter=200,
            random_state=RANDOM_STATE
        ))
    ])
    
    hgb_pipeline.fit(X_train, y_train)
    y_pred_hgb = hgb_pipeline.predict(X_test)
    y_pred_proba_hgb = hgb_pipeline.predict_proba(X_test)[:, 1]
    
    print("HGB TEST SET RESULTS:")
    print(classification_report(y_test, y_pred_hgb, digits=4))
    
    # === MLP Neural Network ===
    print("\n" + "="*70)
    print("PROPOSED: Multi-Layer Perceptron (MLP)")
    print("="*70)
    
    from sklearn.neural_network import MLPClassifier
    from sklearn.preprocessing import StandardScaler as Scaler
    
    # Preprocessing for MLP
    scaler = Scaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)
    X_test_scaled = scaler.transform(X_test)
    
    # MLP with early stopping via validation set
    mlp = MLPClassifier(
        hidden_layer_sizes=(128, 64),           # 2 hidden layers: 128 and 64 neurons
        max_iter=1000,
        learning_rate_init=0.001,
        early_stopping=True,
        validation_fraction=0.1,                # Use 10% of training for early stop
        n_iter_no_change=50,                    # Stop if no improvement for 50 iterations
        batch_size=32,
        random_state=RANDOM_STATE,
        verbose=1
    )
    
    print("Training MLP with early stopping...")
    mlp.fit(X_train_scaled, y_train)
    
    print(f"Training stopped at iteration: {mlp.n_iter_}")
    print(f"Loss history length: {len(mlp.loss_curve_)}")
    
    # Predictions
    y_pred_mlp = mlp.predict(X_test_scaled)
    y_pred_proba_mlp = mlp.predict_proba(X_test_scaled)[:, 1]
    
    print("MLP TEST SET RESULTS:")
    print(classification_report(y_test, y_pred_mlp, digits=4))
    
    # === Comparison ===
    print("\n" + "="*70)
    print("COMPARISON: MLP vs. HistGradientBoosting")
    print("="*70)
    
    from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score
    
    metrics = {}
    for name, y_pred, y_proba in [
        ("HistGradientBoosting", y_pred_hgb, y_pred_proba_hgb),
        ("MLP", y_pred_mlp, y_pred_proba_mlp)
    ]:
        metrics[name] = {
            "precision": precision_score(y_test, y_pred, zero_division=0),
            "recall": recall_score(y_test, y_pred, zero_division=0),
            "f1": f1_score(y_test, y_pred, zero_division=0),
            "roc_auc": roc_auc_score(y_test, y_proba)
        }
    
    comparison_df = pd.DataFrame(metrics).T
    print("\nComparison Table:")
    print(comparison_df.to_string())
    
    # Save comparison
    comparison_df.to_csv(CACHE_DIR / "mlp_vs_baseline.csv")
    
    # === Visualization ===
    print("\nGenerating comparison visualization...")
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Confusion matrices
    from sklearn.metrics import confusion_matrix
    cm_hgb = confusion_matrix(y_test, y_pred_hgb)
    cm_mlp = confusion_matrix(y_test, y_pred_mlp)
    
    sns.heatmap(cm_hgb, annot=True, fmt='d', cmap='Blues', ax=axes[0], cbar=False)
    axes[0].set_title("HistGradientBoosting\nConfusion Matrix")
    axes[0].set_ylabel("Actual")
    axes[0].set_xlabel("Predicted")
    
    sns.heatmap(cm_mlp, annot=True, fmt='d', cmap='Blues', ax=axes[1], cbar=False)
    axes[1].set_title("MLP\nConfusion Matrix")
    axes[1].set_ylabel("Actual")
    axes[1].set_xlabel("Predicted")
    
    plt.tight_layout()
    plt.savefig(CACHE_DIR / "mlp_comparison.png", dpi=150, bbox_inches='tight')
    plt.close()
    
    # Learning curve
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(mlp.loss_curve_, marker='o', markersize=3, label='MLP Training Loss')
    ax.set_xlabel("Iteration")
    ax.set_ylabel("Loss")
    ax.set_title("MLP Learning Curve (with Early Stopping)")
    ax.grid(True, alpha=0.3)
    ax.legend()
    plt.tight_layout()
    plt.savefig(CACHE_DIR / "mlp_learning_curve.png", dpi=150, bbox_inches='tight')
    plt.close()
    
    # === Summary ===
    elapsed = time.time() - start_time
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    print(f"\nFeasibility Assessment:")
    print(f"  GBM Baseline F1: {metrics['HistGradientBoosting']['f1']:.4f}")
    print(f"  MLP F1:          {metrics['MLP']['f1']:.4f}")
    f1_diff = metrics['MLP']['f1'] - metrics['HistGradientBoosting']['f1']
    print(f"  Difference:      {f1_diff:+.4f}")
    
    if f1_diff > 0.01:
        print(f"\n  ✓ MLP shows improvement. Consider adopting for next iteration.")
    elif f1_diff > -0.01:
        print(f"\n  ≈ MLP performance matches GBM. No significant advantage.")
        print(f"    Recommendation: Continue with GBM (simpler, faster, interpretable).")
    else:
        print(f"\n  ✗ MLP underperforms GBM. Conclusion: Non-linearity not needed here.")
        print(f"    Recommendation: Stick with GBM; hand-engineered features capture signal well.")
    
    print(f"\nTraining time: {elapsed:.2f}s")
    print(f"Visualizations saved to {CACHE_DIR}/")

# Assignment 12 B


def fen_to_board_tensor(fen, device='cpu'):
    """
    Convert FEN string to 12-channel 8x8 tensor (one channel per piece type × color).
    """
    board = chess.Board(fen)
    board_array = np.zeros((12, 8, 8), dtype=np.float32)
    
    piece_to_channel = {
        'P': 0, 'N': 1, 'B': 2, 'R': 3, 'Q': 4, 'K': 5,
    }
    
    for square in chess.SQUARES:
        piece = board.piece_at(square)
        if piece is not None:
            rank, file = divmod(square, 8)
            piece_char = piece.symbol().upper()
            channel = piece_to_channel[piece_char]
            
            if not piece.color:
                channel += 6
            
            board_array[channel, 7 - rank, file] = 1.0
    
    tensor = torch.from_numpy(board_array).unsqueeze(0).to(device)
    return tensor

def fen_list_to_tensors(fen_list, device='cpu'):
    """Convert list of FENs to batch tensor."""
    tensors = []
    for fen in tqdm(fen_list, desc="Converting FENs to tensors", disable=len(fen_list)<1000):
        tensors.append(fen_to_board_tensor(fen, device=device).squeeze(0))
    return torch.stack(tensors)

class ChessCNN(nn.Module):
    """Lightweight CNN for chess position classification."""
    def __init__(self, input_channels=12, num_classes=2):
        super(ChessCNN, self).__init__()
        
        self.conv1 = nn.Conv2d(input_channels, 32, kernel_size=3, padding=1)
        self.bn1 = nn.BatchNorm2d(32)
        self.relu1 = nn.ReLU()
        self.pool1 = nn.MaxPool2d(kernel_size=2, stride=2)
        
        self.conv2 = nn.Conv2d(32, 64, kernel_size=3, padding=1)
        self.bn2 = nn.BatchNorm2d(64)
        self.relu2 = nn.ReLU()
        self.pool2 = nn.MaxPool2d(kernel_size=2, stride=2)
        
        self.fc1 = nn.Linear(64 * 2 * 2, 128)
        self.relu_fc = nn.ReLU()
        self.dropout = nn.Dropout(0.5)
        self.fc2 = nn.Linear(128, num_classes)
    
    def forward(self, x):
        x = self.conv1(x)
        x = self.bn1(x)
        x = self.relu1(x)
        x = self.pool1(x)
        
        x = self.conv2(x)
        x = self.bn2(x)
        x = self.relu2(x)
        x = self.pool2(x)
        
        x = x.view(x.size(0), -1)
        
        x = self.fc1(x)
        x = self.relu_fc(x)
        x = self.dropout(x)
        x = self.fc2(x)
        
        return x

def train_cnn():
    """Train CNN and compare to GBM baseline."""
    start_time = time.time()
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Using device: {device}")
    
    print("\nLoading datasets...")
    train_df, val_df, test_df = fetch_dataframes()
    
    print("\nConverting board positions to tensors...")
    X_train_tensor = fen_list_to_tensors(train_df['fen'].values.tolist(), device=device)
    X_val_tensor = fen_list_to_tensors(val_df['fen'].values.tolist(), device=device)
    X_test_tensor = fen_list_to_tensors(test_df['fen'].values.tolist(), device=device)
    
    y_train = torch.from_numpy(train_df['label_engine'].values).long().to(device)
    y_val = torch.from_numpy(val_df['label_engine'].values).long().to(device)
    y_test = torch.from_numpy(test_df['label_engine'].values).long().to(device)
    
    print(f"Train tensor shape: {X_train_tensor.shape}")
    print(f"Val tensor shape:   {X_val_tensor.shape}")
    print(f"Test tensor shape:  {X_test_tensor.shape}")
    print(f"Class distribution (train): {torch.bincount(y_train)}")
    
    train_dataset = TensorDataset(X_train_tensor, y_train)
    val_dataset = TensorDataset(X_val_tensor, y_val)
    test_dataset = TensorDataset(X_test_tensor, y_test)
    
    batch_size = 64
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True, num_workers=0)
    val_loader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False, num_workers=0)
    test_loader = DataLoader(test_dataset, batch_size=batch_size, shuffle=False, num_workers=0)
    
    print(f"\nDataLoaders created: batch_size={batch_size}")
    print(f"  Train batches: {len(train_loader)}")
    print(f"  Val batches:   {len(val_loader)}")
    print(f"  Test batches:  {len(test_loader)}")
    
    print("\n" + "="*70)
    print("BASELINE: HistGradientBoosting Classifier (Hand-Engineered Features)")
    print("="*70)
    
    X_train_feat = prepare_features(train_df)
    X_val_feat = prepare_features(val_df)
    X_test_feat = prepare_features(test_df)
    
    y_train_np = train_df["label_engine"].values
    y_val_np = val_df["label_engine"].values
    y_test_np = test_df["label_engine"].values
    
    tree_preprocess = Pipeline([
        ('imputer', SimpleImputer(strategy='mean')),
        ('scaler', StandardScaler()),
    ])
    
    hgb_pipeline = Pipeline([
        ("preprocess", tree_preprocess),
        ("clf", HistGradientBoostingClassifier(
            max_depth=6,
            learning_rate=0.1,
            max_iter=200,
            random_state=RANDOM_STATE
        ))
    ])
    
    print("Training HistGradientBoosting...")
    hgb_pipeline.fit(X_train_feat, y_train_np)
    y_pred_hgb = hgb_pipeline.predict(X_test_feat)
    y_pred_proba_hgb = hgb_pipeline.predict_proba(X_test_feat)[:, 1]
    
    print("\nHistGradientBoosting Test Results:")
    print(classification_report(y_test_np, y_pred_hgb, digits=4))
    
    print("\n" + "="*70)
    print("PROPOSED: Convolutional Neural Network (Raw Board Representation)")
    print("="*70)
    
    model = ChessCNN(input_channels=12, num_classes=2).to(device)
    print(f"\nModel architecture:")
    print(model)
    
    criterion = nn.CrossEntropyLoss(weight=torch.tensor([1.0, 1.0], device=device))
    optimizer = optim.Adam(model.parameters(), lr=0.001)
    scheduler = optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode='min', factor=0.5, patience=5)
    
    best_val_loss = float('inf')
    patience = 10
    patience_counter = 0
    
    max_epochs = 100
    train_losses = []
    val_losses = []
    
    print(f"\nTraining CNN for up to {max_epochs} epochs...")
    
    for epoch in range(max_epochs):
        model.train()
        train_loss = 0.0
        train_correct = 0
        train_total = 0
        
        for inputs, labels in train_loader:
            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
            
            train_loss += loss.item() * inputs.size(0)
            _, predicted = torch.max(outputs.data, 1)
            train_total += labels.size(0)
            train_correct += (predicted == labels).sum().item()
        
        train_loss /= train_total
        train_acc = train_correct / train_total
        train_losses.append(train_loss)
        
        model.eval()
        val_loss = 0.0
        val_correct = 0
        val_total = 0
        
        with torch.no_grad():
            for inputs, labels in val_loader:
                outputs = model(inputs)
                loss = criterion(outputs, labels)
                
                val_loss += loss.item() * inputs.size(0)
                _, predicted = torch.max(outputs.data, 1)
                val_total += labels.size(0)
                val_correct += (predicted == labels).sum().item()
        
        val_loss /= val_total
        val_acc = val_correct / val_total
        val_losses.append(val_loss)
        
        if (epoch + 1) % 5 == 0 or epoch == 0:
            print(f"Epoch {epoch+1:3d} | Train Loss: {train_loss:.4f} | Train Acc: {train_acc:.4f} | "
                  f"Val Loss: {val_loss:.4f} | Val Acc: {val_acc:.4f}")
        
        scheduler.step(val_loss)
        
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            patience_counter = 0
            torch.save(model.state_dict(), CACHE_DIR / "cnn_best_model.pt")
        else:
            patience_counter += 1
            if patience_counter >= patience:
                print(f"\nEarly stopping triggered at epoch {epoch+1}")
                break
    
    model.load_state_dict(torch.load(CACHE_DIR / "cnn_best_model.pt"))
    
    print("\n" + "="*70)
    print("CNN TEST SET EVALUATION")
    print("="*70)
    
    model.eval()
    y_pred_cnn = []
    y_pred_proba_cnn = []
    
    with torch.no_grad():
        for inputs, labels in test_loader:
            outputs = model(inputs)
            probs = torch.softmax(outputs, dim=1)
            _, predicted = torch.max(outputs.data, 1)
            y_pred_cnn.extend(predicted.cpu().numpy())
            y_pred_proba_cnn.extend(probs[:, 1].cpu().numpy())
    
    y_pred_cnn = np.array(y_pred_cnn)
    y_pred_proba_cnn = np.array(y_pred_proba_cnn)
    
    print("\nCNN Test Results:")
    print(classification_report(y_test_np, y_pred_cnn, digits=4))
    
    print("\n" + "="*70)
    print("COMPARISON: CNN (Raw Board) vs. HistGradientBoosting (Hand-Engineered)")
    print("="*70)
    
    metrics_cnn = {
        "precision": precision_score(y_test_np, y_pred_cnn, zero_division=0),
        "recall": recall_score(y_test_np, y_pred_cnn, zero_division=0),
        "f1": f1_score(y_test_np, y_pred_cnn, zero_division=0),
        "roc_auc": roc_auc_score(y_test_np, y_pred_proba_cnn)
    }
    
    metrics_hgb = {
        "precision": precision_score(y_test_np, y_pred_hgb, zero_division=0),
        "recall": recall_score(y_test_np, y_pred_hgb, zero_division=0),
        "f1": f1_score(y_test_np, y_pred_hgb, zero_division=0),
        "roc_auc": roc_auc_score(y_test_np, y_pred_proba_hgb)
    }
    
    comparison_df = pd.DataFrame({
        "HistGradientBoosting": metrics_hgb,
        "CNN (Raw Board)": metrics_cnn
    })
    
    comparison_df["Difference (CNN - HGB)"] = comparison_df["CNN (Raw Board)"] - comparison_df["HistGradientBoosting"]
    
    print("\nComparison Table:")
    print(comparison_df.to_string())
    
    comparison_df.to_csv(CACHE_DIR / "cnn_vs_baseline.csv")
    print(f"\nComparison saved to {CACHE_DIR / 'cnn_vs_baseline.csv'}")
    
    print("\nGenerating visualizations...")
    
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(train_losses, marker='o', markersize=3, label='Training Loss', alpha=0.7)
    ax.plot(val_losses, marker='s', markersize=3, label='Validation Loss', alpha=0.7)
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Loss")
    ax.set_title("CNN Learning Curves (Training vs. Validation)")
    ax.grid(True, alpha=0.3)
    ax.legend()
    plt.tight_layout()
    plt.savefig(CACHE_DIR / "cnn_learning_curves.png", dpi=150, bbox_inches='tight')
    plt.close()
    
    cm_hgb = confusion_matrix(y_test_np, y_pred_hgb)
    cm_cnn = confusion_matrix(y_test_np, y_pred_cnn)
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    sns.heatmap(cm_hgb, annot=True, fmt='d', cmap='Blues', ax=axes[0], cbar=False)
    axes[0].set_title("HistGradientBoosting\n(Hand-Engineered Features)")
    axes[0].set_ylabel("Actual")
    axes[0].set_xlabel("Predicted")
    
    sns.heatmap(cm_cnn, annot=True, fmt='d', cmap='Greens', ax=axes[1], cbar=False)
    axes[1].set_title("CNN\n(Raw 8×8 Board)")
    axes[1].set_ylabel("Actual")
    axes[1].set_xlabel("Predicted")
    
    plt.tight_layout()
    plt.savefig(CACHE_DIR / "cnn_confusion_matrices.png", dpi=150, bbox_inches='tight')
    plt.close()
    
    fig, ax = plt.subplots(figsize=(10, 6))
    x = np.arange(4)
    width = 0.35
    
    metrics_to_plot = ["precision", "recall", "f1", "roc_auc"]
    hgb_vals = [metrics_hgb[m] for m in metrics_to_plot]
    cnn_vals = [metrics_cnn[m] for m in metrics_to_plot]
    
    ax.bar(x - width/2, hgb_vals, width, label='HGB (Hand-Engineered)', alpha=0.8)
    ax.bar(x + width/2, cnn_vals, width, label='CNN (Raw Board)', alpha=0.8)
    
    ax.set_ylabel('Score')
    ax.set_title('Model Comparison: Metrics')
    ax.set_xticks(x)
    ax.set_xticklabels(metrics_to_plot)
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    plt.savefig(CACHE_DIR / "cnn_metrics_comparison.png", dpi=150, bbox_inches='tight')
    plt.close()
    
    elapsed = time.time() - start_time
    
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    print(f"\nCNN F1 Score:           {metrics_cnn['f1']:.4f}")
    print(f"HGB F1 Score:           {metrics_hgb['f1']:.4f}")
    print(f"Difference:             {metrics_cnn['f1'] - metrics_hgb['f1']:+.4f}")
    print(f"\nCNN ROC-AUC:            {metrics_cnn['roc_auc']:.4f}")
    print(f"HGB ROC-AUC:            {metrics_hgb['roc_auc']:.4f}")
    print(f"Difference:             {metrics_cnn['roc_auc'] - metrics_hgb['roc_auc']:+.4f}")
    
    print(f"\nTraining completed in {elapsed:.2f}s")
    print(f"Outputs saved to {CACHE_DIR}/")
    print(f"  - cnn_vs_baseline.csv")
    print(f"  - cnn_learning_curves.png")
    print(f"  - cnn_confusion_matrices.png")
    print(f"  - cnn_metrics_comparison.png")
    print(f"  - cnn_best_model.pt (PyTorch model weights)")
    
    return {
        "model": model,
        "metrics_cnn": metrics_cnn,
        "metrics_hgb": metrics_hgb,
        "comparison_df": comparison_df,
        "train_losses": train_losses,
        "val_losses": val_losses,
        "y_pred_cnn": y_pred_cnn,
        "y_pred_hgb": y_pred_hgb
    }

if __name__ == "__main__":
    train_cnn()

