from pathlib import Path
import chess.pgn
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from tqdm import tqdm
import time
import os

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

# Assignment 4B, Regression

def prepare_data(df):
    """Prepare data by converting to numeric and handling missing values."""
    df = df.copy()
    df["white_elo"] = pd.to_numeric(df["white_elo"], errors="coerce")
    df["black_elo"] = pd.to_numeric(df["black_elo"], errors="coerce")
    
    # Calculate Elo difference (positive = white is higher rated)
    df["rating_diff"] = df["white_elo"] - df["black_elo"]
    
    df["result_num"] = df["result"].map({
        "1-0": 1.0,
        "0-1": 0.0,
        "1/2-1/2": 0.5
    })
    
    return df

def create_preprocessing_pipeline():
    """Create a preprocessing pipeline with imputation and scaling."""
    pipeline = Pipeline([
        ('imputer', SimpleImputer(strategy='mean')),
        ('scaler', StandardScaler()),
        ('regressor', LinearRegression())
    ])
    return pipeline

def lin_regress(df):
    """Train linear regression model using Elo rating difference as feature."""
    df = prepare_data(df)
    
    # Remove rows with missing target and rating_diff
    df = df.dropna(subset=['result_num', 'rating_diff'])
    
    # Feature: Elo rating difference
    X = df[["rating_diff"]]
    y = df["result_num"]
    
    # Create and fit the pipeline
    pipeline = create_preprocessing_pipeline()
    pipeline.fit(X, y)
    
    # Get predictions
    preds = pipeline.predict(X)
    
    return preds, pipeline


def _add_bias_column(X: np.ndarray) -> np.ndarray:
    X = np.asarray(X)
    if X.ndim == 1:
        X = X.reshape(-1, 1)
    return np.hstack([np.ones((X.shape[0], 1)), X])


def _predict_weights(X: np.ndarray, w: np.ndarray) -> np.ndarray:
    Xb = _add_bias_column(X)
    return Xb.dot(w)


def _mse_loss_weights(Xb: np.ndarray, y: np.ndarray, w: np.ndarray) -> float:
    return float(np.mean((Xb.dot(w) - y) ** 2))


def _mse_grad_weights(Xb: np.ndarray, y: np.ndarray, w: np.ndarray) -> np.ndarray:
    n = Xb.shape[0]
    return (2.0 / n) * (Xb.T.dot(Xb.dot(w) - y))


def train_gd(X: np.ndarray, y: np.ndarray, lr: float = 1e-3, epochs: int = 2000, random_state: int = 42):
    """Train linear regression with gradient descent on feature matrix X and target y.

    Returns: weights, losses_list
    """
    X = np.asarray(X, dtype=float)
    y = np.asarray(y, dtype=float)
    if X.ndim == 1:
        X = X.reshape(-1, 1)

    Xb = _add_bias_column(X)

    rng = np.random.default_rng(random_state)
    w = rng.normal(0, 0.01, size=(Xb.shape[1],))

    losses = []
    for epoch in range(epochs):
        grad = _mse_grad_weights(Xb, y, w)
        w = w - lr * grad
        losses.append(_mse_loss_weights(Xb, y, w))

    return w, losses


def visualize_loss(losses, out_path="loss_curve_gd.png"):
    plt.figure()
    plt.plot(losses, label="train")
    plt.xlabel("epoch")
    plt.ylabel("MSE")
    plt.title("Gradient Descent Training Loss")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close()


def lin_regress_gd(df, lr: float = 1e-3, epochs: int = 2000):
    """Train using gradient descent on Elo rating difference (single feature).

    Returns: weights, train_preds, losses
    """
    df = prepare_data(df)
    df = df.dropna(subset=["result_num", "rating_diff"]).copy()

    X = df[["rating_diff"]].values.astype(float)
    y = df["result_num"].values.astype(float)

    w, losses = train_gd(X, y, lr=lr, epochs=epochs)
    preds = _predict_weights(X, w)
    return w, preds, losses

def compute_metrics(y_true, y_pred):
    """Compute MSE, RMSE, MAE, and R2 score."""
    mse = mean_squared_error(y_true, y_pred)
    rmse = np.sqrt(mse)
    mae = mean_absolute_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)
    return {'MSE': mse, 'RMSE': rmse, 'MAE': mae, 'R2': r2}

def visualize_metrics(train_metrics, val_metrics, test_metrics):
    """Create comprehensive visualizations for all metrics."""
    fig, axes = plt.subplots(2, 2, figsize=(12, 10))
    fig.suptitle('Model Evaluation Metrics', fontsize=16)
    
    metrics_names = ['MSE', 'RMSE', 'MAE', 'R2']
    
    for idx, (ax, metric) in enumerate(zip(axes.flat, metrics_names)):
        datasets = ['Train', 'Val', 'Test']
        values = [train_metrics[metric], val_metrics[metric], test_metrics[metric]]
        colors = ['#3498db', '#2ecc71', '#e74c3c']
        
        bars = ax.bar(datasets, values, color=colors, alpha=0.7, edgecolor='black')
        ax.set_ylabel(metric, fontsize=11)
        ax.set_title(f'{metric} Across Datasets', fontsize=12)
        ax.grid(axis='y', alpha=0.3)
        
        # Add value labels on bars
        for bar, value in zip(bars, values):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                    f'{value:.4f}', ha='center', va='bottom', fontsize=10)
    
    plt.tight_layout()
    plt.savefig("metrics_comparison.png", dpi=200)
    plt.close()

def print_metrics_summary(train_metrics, val_metrics, test_metrics):
    """Print a formatted summary of all metrics."""
    print("\n" + "="*70)
    print("MODEL EVALUATION METRICS")
    print("="*70)
    
    metrics_names = ['MSE', 'RMSE', 'MAE', 'R2']
    print(f"{'Metric':<10} {'Train':<15} {'Validation':<15} {'Test':<15}")
    print("-"*70)
    
    for metric in metrics_names:
        train_val = train_metrics[metric]
        val_val = val_metrics[metric]
        test_val = test_metrics[metric]
        print(f"{metric:<10} {train_val:<15.6f} {val_val:<15.6f} {test_val:<15.6f}")
    
    print("="*70 + "\n")






def main():
    start_time = time.time()
    games_df, train_df, val_df, test_df, train_ids, val_ids, test_ids = load_or_create_datasets()

    total_elapsed = time.time() - start_time
    print(f"\nCompleted in {total_elapsed:.2f}s total")

    train_games = games_df[games_df["game_id"].isin(train_ids)]
    val_games = games_df[games_df["game_id"].isin(val_ids)]
    test_games = games_df[games_df["game_id"].isin(test_ids)]

    # Train the model
    print("\nTraining linear regression model with Elo rating difference...")
    train_games_prep = prepare_data(train_games)
    train_games_prep = train_games_prep.dropna(subset=['result_num', 'rating_diff'])
    train_preds, pipeline = lin_regress(train_games)
    print(f"Training completed. Model pipeline:\n{pipeline}")
    
    # Evaluate on all datasets
    print("\nEvaluating on all datasets...")
    
    # Validation
    val_processed = prepare_data(val_games)
    val_processed = val_processed.dropna(subset=['result_num', 'rating_diff'])
    val_X = val_processed[["rating_diff"]]
    val_preds = pipeline.predict(val_X)
    
    # Test
    test_processed = prepare_data(test_games)
    test_processed = test_processed.dropna(subset=['result_num', 'rating_diff'])
    test_X = test_processed[["rating_diff"]]
    test_preds = pipeline.predict(test_X)
    
    # Compute metrics
    train_y = train_games_prep["result_num"].values
    val_y = val_processed["result_num"].values
    test_y = test_processed["result_num"].values
    
    train_metrics = compute_metrics(train_y, train_preds)
    val_metrics = compute_metrics(val_y, val_preds)
    test_metrics = compute_metrics(test_y, test_preds)
    
    # Print and visualize results
    print_metrics_summary(train_metrics, val_metrics, test_metrics)
    visualize_metrics(train_metrics, val_metrics, test_metrics)



if __name__ == "__main__":
    main()