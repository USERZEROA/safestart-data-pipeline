from __future__ import annotations

import json

import joblib
import numpy as np
import polars as pl
import pytest

from ml_fixtures import generated_lake, synthetic_panel
from safestart_pipeline.ml.__main__ import main
from safestart_pipeline.ml.features import FEATURES, TARGET
from safestart_pipeline.ml.train import MODELS, Split, split_panel, train

METRICS = {"rows", "positives", "base_rate", "roc_auc", "pr_auc", "precision_at_5pct", "recall_at_5pct", "brier",
           "mean_predicted", "calibration_deciles"}


@pytest.fixture(scope="module")
def panel() -> pl.DataFrame:
    return synthetic_panel()


def test_the_split_is_by_time(panel):
    parts = split_panel(panel)
    assert parts["train"]["year"].max() == 2021 and parts["validate"]["year"].unique().to_list() == [2022]
    assert parts["test"]["year"].unique().sort().to_list() == [2023, 2024]
    # Every training quarter comes before every validation quarter, and every validation quarter before every test one.
    assert parts["train"]["quarter"].max() < parts["validate"]["quarter"].min()
    assert parts["validate"]["quarter"].max() < parts["test"]["quarter"].min()
    # Each labeled row through 2024 is in exactly one part; the years after it and the open quarter are in none.
    assert sum(part.height for part in parts.values()) == panel.filter(pl.col("year") <= 2024).height
    assert all(part[TARGET].null_count() == 0 for part in parts.values())


def test_the_split_years_can_move(panel):
    parts = split_panel(panel, Split(train_through=2010, validate=2011, test=(2012, 2013)))
    years = (parts["train"]["year"].max(), parts["validate"]["year"].min(), parts["test"]["year"].max())
    assert years == (2010, 2011, 2013)


def test_training_writes_the_model_metrics_importances_and_scores(panel, tmp_path):
    metrics = train(panel, tmp_path)
    assert set(metrics["models"]) == set(MODELS) and metrics["selected"] in MODELS
    for result in metrics["models"].values():
        assert set(result["validate"]) >= METRICS and set(result["test"]) >= METRICS
        assert result["test"]["roc_auc"] > 0.6  # the synthetic target depends on hours worked and past accidents
        assert 0 <= result["test"]["precision_at_5pct"] <= 1 and len(result["test"]["calibration_deciles"]) <= 10
    assert metrics["split"]["test"]["quarters"] == "2023Q1 to 2024Q4"
    assert metrics["final_model"]["fitted_on"] == "2001Q1 to 2025Q4"
    assert json.loads((tmp_path / "metrics.json").read_text(encoding="utf-8")) == metrics

    scores = pl.read_parquet(tmp_path / "risk_scores.parquet")
    assert scores.height == 150 and scores["period"].unique().to_list() == ["2026Q1"]
    assert scores["risk_rank"].to_list() == list(range(1, 151)) and scores["risk_score"].is_sorted(descending=True)
    bundle = joblib.load(tmp_path / "model.joblib")
    assert bundle["features"] == list(FEATURES) and bundle["selected"] == metrics["selected"]
    open_quarter = panel.filter(pl.col(TARGET).is_null()).join(scores.select("mine_id", "risk_score"), on="mine_id")
    assert np.allclose(bundle["model"].predict_proba(open_quarter.select(FEATURES))[:, 1], open_quarter["risk_score"])

    importance = pl.read_csv(tmp_path / "permutation_importance.csv")
    assert set(importance["model"]) == set(MODELS) and importance.height == len(MODELS) * len(FEATURES)
    top = importance.filter(pl.col("model") == metrics["selected"])["feature"][0]
    assert top in ("hours_worked_4q", "hours_worked_1q", "avg_employees_4q", "serious_accidents_4q")


def test_training_needs_both_outcomes_in_every_part(tmp_path):
    with pytest.raises(ValueError, match="the validate quarters need both outcomes"):
        train(synthetic_panel(mines=20, last_year=2021), tmp_path)


def test_the_command_trains_from_a_lake_and_writes_the_analysis(tmp_path, capsys):
    lake = generated_lake(tmp_path / "data")
    assert main(["train", "--lake", str(lake), "--out", str(tmp_path / "model")]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["scores"] == {"period": "2025Q3", "mines": 40} and set(result["test"]) == set(MODELS)
    metrics = json.loads((tmp_path / "model" / "metrics.json").read_text(encoding="utf-8"))
    assert metrics["run"]["lake"]["violations"] == "v1.parquet" and metrics["run"]["peak_memory_mb"] > 0
    scores = pl.read_parquet(tmp_path / "model" / "risk_scores.parquet")
    assert scores.columns[:5] == ["mine_id", "mine_name", "status", "period", "risk_score"]

    assert main(["analyze", "--lake", str(lake), "--out", str(tmp_path / "analysis")]) == 0
    assert json.loads(capsys.readouterr().out)["rows"]["top_cited_sections"] == 3
