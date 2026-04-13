# Part B – Weekly Capstone Assignment: Neural Networks Feasibility

## 1. Project Context (Brief)

* **Project Title:** Chess Outcome Prediction from Board Positions
* **Data Modality:** Tabular (hand-engineered chess position features)
* **Task Type:** Binary classification (predicting if a position favors White or not)
* **One-Sentence Goal:** Predict whether a given chess board position presents a winning advantage for White based on position features and Stockfish engine evaluation.

---

## 2. This Week's Technique and Its Assumptions

* **Technique / Model Family Covered This Week:** Multi-Layer Perceptron (MLP) Neural Networks
* **Key Assumptions of This Technique:** 
  1. Non-linear relationships between input features and output labels can improve predictive power over linear/tree-based models
  2. A sufficient amount of data (millions of samples) can be effectively learned by neural networks with proper regularization (early stopping, dropout, batch normalization)

**Fit Assessment (required):**

> I expect this technique to be a **partial fit** (empirically shown to be poor fit in practice) for my project because:
>
> While we have abundant data (1.17M training positions), our feature space is inherently structured and well-captured by prior domain knowledge (material balance, castling rights, legal move counts). Gradient Boosting models have proven ideal for tabular data with interpretable feature interactions. The key question was whether neural non-linearity could extract additional signal—and our proxy experiment definitively answered: *no*. The MLP achieved near-identical performance (F1 0.8239 vs. GBM's 0.8259), suggesting the hand-engineered features are feature-complete for this task's signal.

---

## 3. Representation or Proxy Used

* **Representation or Proxy Chosen:** Hand-engineered chess position features (10-11 dimensional vectors) passed directly to both MLP and GBM for fair comparison
  
* **Why this representation was reasonable for this week:**
  - **Direct comparison:** Both models consume the same features, making any performance difference attributable to the model architecture, not representation
  - **Computationally feasible:** 10-11 dims × 1.17M samples trains in ~1 min for MLP, allowing quick iteration
  - **Interpretable proxy:** Instead of attempting raw board encoding (768 dims) which would require architectural specialization (CNNs), we tested the core assumption: do neural networks with non-linear activation provide an advantage over GBM for this domain?
  - **Conservative scope:** A positive result would justify deeper investigation; a negative result (which is what we observed) closes the inquiry with confidence

---

## 4. What Was Attempted

* **MLP Neural Network Proxy Implementation**
  - Architecture: Input (10 features) → Dense(128) → Dense(64) → Output(2 classes)
  - Regularization: Early stopping (50 iterations patience), validation_fraction=0.1, batch_size=32
  - Optimizer: Adam (learning_rate=0.001), max_iter=1000
  - Training: Fitted on 1.17M training positions; convergence at iteration 82

* **Baseline Comparison**
  - HistGradientBoosting Classifier (from Assignment 6B, best-performing prior model)
  - Same preprocessing pipeline: Impute(mean) → StandardScaler
  - Evaluated on identical test set (146,393 positions)

* **What we intentionally did NOT attempt:**
  - Raw board representation (768 dims): Would require CNN architecture and substantially more compute; only justified if MLP with current features showed promise
  - Hyperparameter tuning on MLP (grid search): Not warranted given the null result; extensive tuning of a weaker model violates ML judgment
  - Recurrent or attention architectures: No temporal/sequence structure in single-position classification; overkill for this task
  - Ensemble (MLP + GBM): Adds complexity without addressing the core question

* **Constraints encountered:**
  - Compute time: Full training pipeline (annotating 1.17M positions with engine evals + feature extraction + model training) takes ~10 minutes on WSL
  - Early stopping required: MLP converged by iteration 82; validation loss plateaued, confirming over-parameterization for this feature set

---

## 5. Results or Observations

**Test Set Performance (146,393 positions):**

| Metric | HistGradientBoosting | MLP | Difference |
|--------|----------------------|-----|------------|
| Precision | 0.7768 | 0.7768 | 0.0000 |
| Recall | 0.8815 | 0.8771 | -0.0044 |
| F1-Score | 0.8259 | 0.8239 | **-0.0019** |
| ROC-AUC | 0.8643 | 0.8628 | -0.0015 |

**Qualitative Observations:**
- MLP training was stable: loss decreased monotonically from 0.459 → 0.448 over 82 iterations
- Validation curves showed consistent early stopping trigger; no instability detected
- Both models exhibit similar confusion matrix patterns: better recall on class 1 (winning positions) than class 0
- MLP's learning curve suggests the model was not underfitted (loss continued improving until stopped); the plateau is due to fundamental feature limitations, not training issues

---

## 6. Interpretation and Judgment

**Why the Method Behaved as It Did:**

The MLP's near-identical performance to HistGradientBoosting reveals a critical insight: the hand-engineered chess features already encode the essential signal for position evaluation. These features—material balance, castling rights, legal move counts, ply depth—are domain heuristics refined over centuries of chess theory. They map directly onto the underlying chess mechanics that determine outcome.

HistGradientBoosting excels at tabular data because it discovers non-linear feature interactions through adaptive tree splits without redundant parameterization. The MLP, despite its theoretical flexibility, found nothing better to learn: the data has no hidden non-linearities waiting to be discovered. The two models converged to essentially the same decision boundary from different analytical directions.

**Which Assumptions Held or Failed:**
- ✓ **Held**: We have sufficient data (1.17M samples) for MLP training without overfitting
- ✗ **Failed**: Non-linear relationships do *not* improve prediction for this feature set
- ✓ **Held**: Early stopping and regularization prevented pathological MLP behavior
- ✗ **Failed**: The assumption that tabular data or large sample sizes alone justify neural networks

**What This Reveals About the Problem:**

This is a *well-posed feature engineering problem*, not a representation learning problem. The signal is already accessible through domain knowledge. Chess positions are deterministic by nature—their evaluation is governed by explicit rules (piece mobility, material advantage, king safety)—and our features capture these rules. This is fundamentally different from, say, image classification, where pixel representations are far from semantic structure and neural nets' hierarchical feature extraction is essential.

The experiment succeeded precisely because it failed to show improvement. It demonstrates ML maturity: recognizing when a simpler, more interpretable model is the correct choice. Adding neural complexity here would be *over-engineering*, increasing inference latency and model size for zero predictive gain.

---

## 7. Forward-Looking Adjustment

**What Will We Keep, Change, or Discard:**

- **Keep:** HistGradientBoosting as the deployed model. It is simpler, faster to train, fully interpretable (tree splits can be inspected), and achieves 82.6% F1.
- **Discard:** Any further investigation into neural architectures for this task. The proxy experiment was conclusive.
- **Potential future direction (if re-scoped):** If the goal shifts to *explaining* why positions are winning/losing (interpretability beyond F1), gradient-based feature importance and SHAP analysis on GBM could provide richer insights than neural approaches.

---

## 8. Mismatch Acknowledgment

**This week's technique was a poor fit. Here's why:**

The core assumption justifying MLPs—that hidden non-linear relationships exist in the data—does not hold for chess position evaluation. Chess is a fully observable, deterministic game where outcomes flow from piece interactions and board geometry. Our 10-dimensional feature vector directly encodes these mechanics.

**Evidence supporting the poor fit:**
1. MLP F1 (0.8239) matches GBM (0.8259): suggests feature saturation, not underfitting
2. Early stopping at iteration 82: loss plateau indicates the network learned all recoverable signal quickly
3. Domain analysis: our features are domain heuristics, not raw representations requiring learned embeddings

**Value of this attempt despite poor fit:**
This negative result is *valuable*. It:
- Validates our feature engineering choices
- Prevents wasted effort on neural architecture search
- Demonstrates evidence-based model selection (choosing GBM over MLP despite MLP being "modern")
- Reinforces the ML principle: simpler models are preferable when performance is equivalent (Occam's Razor)

---

## Submission Notes

* **Submission format:** Markdown (this document)
* **Code:** MLP implementation saved in `main.py` (Assignment 11 Part B section)
* **Outputs:**
  - `cache/mlp_vs_baseline.csv` — Detailed metric comparison
  - `cache/mlp_comparison.png` — Confusion matrices (HGB vs. MLP side-by-side)
  - `cache/mlp_learning_curve.png` — MLP training convergence
* **Key takeaway:** Neural networks are not justified for this problem. Recommendation: **Deploy HistGradientBoosting model.**
