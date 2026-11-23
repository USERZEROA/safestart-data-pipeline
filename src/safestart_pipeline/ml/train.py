"""Train and evaluate the mine accident-risk model: will a mine have a serious accident in the coming quarter?

The split is by time, never by row: models learn from target quarters through 2021, are compared on 2022 and tested on
2023 and 2024, so every evaluation predicts quarters later than any the model learned from, as in use. A mine appears
in all three periods; what keeps the test honest is that each row's features end before its own quarter.

Two models: a logistic regression on log-scaled counts, the year and one-hot categories (the baseline) and a
histogram gradient boosting classifier with native categories. The one with the higher validation PR AUC is refitted
on every labeled quarter and scores the open quarter, the one after the newest production reports.

Metrics: ROC AUC; PR AUC (average precision, since only 4 to 9 percent of mine-quarters have a serious accident);
precision and recall among the riskiest 5 percent of mine-quarters, the share an inspection program could act on; and
calibration (Brier score, mean predicted against observed rate, observed rate by decile of predicted risk).
Permutation importance is the drop in test PR AUC and ROC AUC when one feature's values are shuffled across rows.
"""

from __future__ import annotations

import ctypes
import json
import logging
import math
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import polars as pl
from sklearn.calibration import calibration_curve
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import FunctionTransformer, OneHotEncoder, OrdinalEncoder, StandardScaler

from ..definitions import SERIOUS_DEGREES
from .features import CATEGORICAL, FEATURES, NUMERIC, TARGET, TREND, period, quarter_label

log = logging.getLogger(__name__)
TOP_SHARE = 0.05  # the riskiest share of mine-quarters for precision and recall
SEED = 0


@dataclass(frozen=True)
class Split:
    """Target years of each part; the years after `test` (and the open quarter) are used only for the final refit."""

    train_through: int = 2021
    validate: int = 2022
    test: tuple[int, int] = (2023, 2024)


def split_panel(panel: pl.DataFrame, split: Split = Split()) -> dict[str, pl.DataFrame]:
    labeled = panel.filter(pl.col(TARGET).is_not_null())
    return {
        "train": labeled.filter(pl.col("year") <= split.train_through),
        "validate": labeled.filter(pl.col("year") == split.validate),
        "test": labeled.filter(pl.col("year").is_between(*split.test)),
    }


def logistic_regression() -> Pipeline:
    log_scale = FunctionTransformer(np.log1p, validate=True, feature_names_out="one-to-one")
    numeric = make_pipeline(log_scale, StandardScaler())
    encode = ColumnTransformer([
        ("numeric", numeric, list(NUMERIC)),
        ("trend", StandardScaler(), list(TREND)),
        ("categorical", OneHotEncoder(handle_unknown="ignore"), list(CATEGORICAL)),
    ])
    return make_pipeline(encode, LogisticRegression(max_iter=2000))


def gradient_boosting() -> Pipeline:
    # Categories become ordinal codes the booster splits on as categories; one unseen in training counts as missing.
    encode = ColumnTransformer(
        [("categorical", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=np.nan), list(CATEGORICAL))],
        remainder="passthrough", verbose_feature_names_out=False,
    ).set_output(transform="polars")
    booster = HistGradientBoostingClassifier(
        categorical_features=list(CATEGORICAL), max_iter=500, learning_rate=0.1, min_samples_leaf=200,
        l2_regularization=1.0, early_stopping=True, n_iter_no_change=20, random_state=SEED,
    )
    return make_pipeline(encode, booster)


MODELS = {"logistic_regression": logistic_regression, "hist_gradient_boosting": gradient_boosting}


def labels(frame: pl.DataFrame) -> np.ndarray:
    return frame[TARGET].cast(pl.Int8).to_numpy()


def quarters(frame: pl.DataFrame) -> str:
    return f"{quarter_label(frame['quarter'].min())} to {quarter_label(frame['quarter'].max())}"


def risk(model: Pipeline, frame: pl.DataFrame) -> np.ndarray:
    return model.predict_proba(frame.select(FEATURES))[:, 1]


def evaluate(target: np.ndarray, score: np.ndarray, top: float = TOP_SHARE) -> dict[str, Any]:
    riskiest = np.argsort(-score, kind="stable")[: max(1, math.ceil(top * len(score)))]
    hits, positives = int(target[riskiest].sum()), int(target.sum())
    observed, predicted = calibration_curve(target, score, n_bins=10, strategy="quantile")
    return {
        "rows": len(target),
        "positives": positives,
        "base_rate": round(float(target.mean()), 5),
        "roc_auc": round(float(roc_auc_score(target, score)), 4),
        "pr_auc": round(float(average_precision_score(target, score)), 4),
        "precision_at_5pct": round(hits / len(riskiest), 4),
        "recall_at_5pct": round(hits / max(positives, 1), 4),
        "brier": round(float(brier_score_loss(target, score)), 5),
        "mean_predicted": round(float(score.mean()), 5),
        "calibration_deciles": [
            {"predicted": round(float(p), 4), "observed": round(float(o), 4)}
            for p, o in zip(predicted, observed, strict=True)
        ],
    }


def permutation_importance(model: Pipeline, frame: pl.DataFrame, repeats: int = 5) -> list[dict[str, Any]]:
    """Mean drop in PR AUC and ROC AUC over `repeats` shuffles of each feature's values across the rows of `frame`."""
    target, features = labels(frame), frame.select(FEATURES)
    base = risk(model, features)
    base_pr, base_roc = average_precision_score(target, base), roc_auc_score(target, base)
    rows = []
    for name in FEATURES:
        drops = []
        for repeat in range(repeats):
            score = risk(model, features.with_columns(features[name].shuffle(seed=SEED + repeat)))
            drops.append((base_pr - average_precision_score(target, score), base_roc - roc_auc_score(target, score)))
        pr_drops, roc_drops = np.array(drops).T
        rows.append({"feature": name, "pr_auc_drop": round(float(pr_drops.mean()), 5),
                     "pr_auc_drop_std": round(float(pr_drops.std()), 5),
                     "roc_auc_drop": round(float(roc_drops.mean()), 5)})
    return sorted(rows, key=lambda row: -row["pr_auc_drop"])


def risk_scores(model: Pipeline, panel: pl.DataFrame, details: pl.DataFrame | None) -> pl.DataFrame:
    """The open quarter's risk for every mine in it, riskiest first, with the window features behind each score."""
    open_quarter = panel.filter(pl.col(TARGET).is_null())
    scores = open_quarter.select(
        "mine_id",
        period().alias("period"),
        pl.Series("risk_score", risk(model, open_quarter)),
        *CATEGORICAL,
        *NUMERIC,
    ).sort("risk_score", "mine_id", descending=[True, False])
    scores = scores.with_columns(pl.int_range(1, scores.height + 1, dtype=pl.Int32).alias("risk_rank"))
    if details is not None:
        scores = scores.join(details, on="mine_id", how="left")
    names = ["mine_name", "status"] if details is not None else []
    leading = ["mine_id", *names, "period", "risk_score", "risk_rank"]
    return scores.select(*leading, pl.exclude(leading))


def peak_memory_mb() -> float:
    """This process's peak resident memory so far (the standard library has no portable call for it)."""
    if sys.platform == "win32":
        from ctypes import wintypes

        sizes = ("PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage", "QuotaPagedPoolUsage",
                 "QuotaPeakNonPagedPoolUsage", "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage")

        class Counters(ctypes.Structure):  # PROCESS_MEMORY_COUNTERS
            _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD)]
            _fields_ += [(name, ctypes.c_size_t) for name in sizes]

        kernel32 = ctypes.WinDLL("kernel32")
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        kernel32.K32GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
        counters = Counters(cb=ctypes.sizeof(Counters))
        kernel32.K32GetProcessMemoryInfo(kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb)
        return round(counters.PeakWorkingSetSize / 2**20, 1)
    import resource

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # KiB on Linux, bytes on macOS
    return round(peak / (2**20 if sys.platform == "darwin" else 2**10), 1)


def train(
    panel: pl.DataFrame, out: Path, *, details: pl.DataFrame | None = None, split: Split = Split(),
    run: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Fit, compare and test both models, refit the better one on every labeled quarter, and write to `out`:
    model.joblib, metrics.json, permutation_importance.csv and risk_scores.parquet (the open quarter's scores).
    `details` (mine_id, mine_name, status, newest_report_missing: `features.mine_details`) labels the scores; `run`
    is added to the metrics as the run record."""
    started = time.perf_counter()
    parts = split_panel(panel, split)
    for name, part in parts.items():
        if part.is_empty() or part[TARGET].n_unique() < 2:
            raise ValueError(f"the {name} quarters need both outcomes; the panel has {part.height} rows there")
    fitted: dict[str, Pipeline] = {}
    results: dict[str, dict[str, Any]] = {}
    for name, make in MODELS.items():
        fit_started = time.perf_counter()
        fitted[name] = make().fit(parts["train"].select(FEATURES), labels(parts["train"]))
        results[name] = {"fit_seconds": round(time.perf_counter() - fit_started, 1)} | {
            part: evaluate(labels(parts[part]), risk(fitted[name], parts[part])) for part in ("validate", "test")
        }
        log.info("%s: fitted in %.1f s, validation PR AUC %.4f", name, results[name]["fit_seconds"],
                 results[name]["validate"]["pr_auc"])
    selected = max(MODELS, key=lambda name: results[name]["validate"]["pr_auc"])
    importance = pl.DataFrame([
        {"model": name, **row} for name in MODELS for row in permutation_importance(fitted[name], parts["test"])
    ])

    labeled = panel.filter(pl.col(TARGET).is_not_null())
    final = MODELS[selected]().fit(labeled.select(FEATURES), labels(labeled))
    log.info("%s refitted on %d labeled mine-quarters", selected, labeled.height)
    scores = risk_scores(final, panel, details)

    out.mkdir(parents=True, exist_ok=True)
    joblib.dump({"model": final, "features": list(FEATURES), "selected": selected, "fitted_on": quarters(labeled)},
                out / "model.joblib")
    importance.write_csv(out / "permutation_importance.csv")
    scores.write_parquet(out / "risk_scores.parquet")
    metrics = {
        "created_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "target": f"a serious accident (degree of injury {', '.join(SERIOUS_DEGREES)}) at the mine in the quarter",
        "features": {"numeric": list(NUMERIC), "trend": list(TREND), "categorical": list(CATEGORICAL)},
        "split": {name: {"quarters": quarters(part), "rows": part.height, "positives": int(part[TARGET].sum())}
                  for name, part in parts.items()},
        "models": results,
        "selected": selected,
        "final_model": {"fitted_on": quarters(labeled), "rows": labeled.height},
        "scores": {"period": scores["period"][0] if scores.height else None, "mines": scores.height},
        "importance_top": importance.filter(pl.col("model") == selected).head(5).to_dicts(),
        "run": (run or {}) | {"train_seconds": round(time.perf_counter() - started, 1),
                              "peak_memory_mb": peak_memory_mb()},
    }
    (out / "metrics.json").write_text(json.dumps(metrics, indent=1), encoding="utf-8")
    return metrics
