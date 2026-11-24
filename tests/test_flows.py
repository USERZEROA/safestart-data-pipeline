"""The batch, model and official flows with every step replaced by a fake (under a temporary Prefect API),
the schedules, and `python -m safestart_pipeline.flows serve|run`."""

import asyncio
import json
import logging
import os
import runpy
import sys
from datetime import UTC, datetime, timedelta, timezone
from functools import partial
from typing import ClassVar

import pytest
from prefect import flow
from prefect.server.schemas.schedules import CronSchedule
from prefect.testing.utilities import prefect_test_harness

from safestart_pipeline.crawl.locks import SiteLock
from safestart_pipeline.flows import __main__ as flows_main
from safestart_pipeline.flows import model as model_flow
from safestart_pipeline.flows import official as official_flow
from safestart_pipeline.flows import pipeline
from safestart_pipeline.flows.schedule import MAX_SLEEP_SECONDS, SUNDAY, Schedule, run_on_schedule
from safestart_pipeline.lake.msha_ogd import LakeBuildError
from safestart_pipeline.official import run as official_run

SATURDAY_NOON = datetime(2026, 11, 21, 12, 0, tzinfo=UTC)
DAILY, WEEKLY = Schedule(hour=14), Schedule(hour=15, weekday=SUNDAY)
OFFICIAL_MINUTES = 12 * 60  # 16:30 to its 04:30 deadline


@pytest.fixture(scope="module", autouse=True)
def prefect_api():
    """A temporary Prefect API, so flow runs never reach a real workspace."""
    with prefect_test_harness():
        yield


@pytest.fixture(autouse=True)
def package_log_level():
    """`main` lets the pipeline's INFO lines through; the other tests keep the level they had."""
    logger = logging.getLogger("safestart_pipeline")
    level = logger.level
    yield
    logger.setLevel(level)


@pytest.fixture
def root(monkeypatch, tmp_path):
    """A temporary data root and warehouse, and retries that start at once instead of minutes later."""
    monkeypatch.setenv("SAFESTART_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("SAFESTART_WAREHOUSE_PATH", str(tmp_path / "warehouse" / "safestart.duckdb"))
    for module, name in ((pipeline, "build_lake_task"), (pipeline, "build_warehouse_task"), (pipeline, "export_task"),
                         (model_flow, "train_task"), (model_flow, "analyze_task")):
        monkeypatch.setattr(module, name, getattr(module, name).with_options(retry_delay_seconds=0))
    return tmp_path


class Steps:
    """Records each call of a fake step; a step named in `failures` raises its exception."""

    def __init__(self):
        self.calls: list[tuple] = []
        self.failures: dict[str, BaseException] = {}

    def __call__(self, name: str, *args: object) -> None:
        self.calls.append((name, *args))
        if name in self.failures:
            raise self.failures[name]

    @property
    def names(self) -> list[str]:
        return [call[0] for call in self.calls]


@pytest.fixture
def batch_steps(monkeypatch, root):
    steps = Steps()

    @flow(name="acquire-bulk-files")
    def acquire():
        steps("acquire")
        return {"msha-ogd": 40}

    def build_lake(data_root):
        steps("lake", data_root)
        return {"mines": {"rows": 3, "versions_built": 1}, "accidents": {"rows": 2, "versions_built": 0}}

    def build(data_root, target):
        steps("warehouse", data_root, target)
        return {"warehouse": str(target), "seconds": 1.0, "rows": {"mine": 3}}

    def export(warehouse, out):
        steps("exports", warehouse, out)
        return {"files": {"mine_profiles.parquet": {"rows": 3}, "cited_sections.parquet": {"rows": 5}}}

    monkeypatch.setattr(pipeline, "acquire_bulk", acquire)
    monkeypatch.setattr("safestart_pipeline.lake.msha_ogd.build_lake", build_lake)
    monkeypatch.setattr("safestart_pipeline.warehouse.build.build", build)
    monkeypatch.setattr("safestart_pipeline.warehouse.exports.export", export)
    return steps


# The batch flow -------------------------------------------------------------------------------------------------------


def test_the_batch_downloads_then_builds_the_lake_the_warehouse_and_the_exports(batch_steps, root):
    summary = pipeline.batch()
    warehouse = root / "warehouse" / "safestart.duckdb"
    assert batch_steps.calls == [("acquire",), ("lake", root), ("warehouse", root, warehouse),
                                 ("exports", warehouse, root / "exports" / "safestart")]
    assert list(summary) == ["acquire", "lake", "warehouse", "exports"]
    assert summary["acquire"] == {"msha-ogd": 40} and summary["warehouse"]["rows"] == {"mine": 3}


def test_without_downloads_the_batch_starts_at_the_lake(batch_steps):
    pipeline.batch(download=False)
    assert batch_steps.names == ["lake", "warehouse", "exports"]


def test_a_failed_download_still_brings_the_rest_up_to_date_then_fails_the_run(batch_steps):
    batch_steps.failures["acquire"] = RuntimeError("2 file(s) failed after retries")
    with pytest.raises(RuntimeError, match="acquire-bulk-files"):
        pipeline.batch()
    assert batch_steps.names == ["acquire", "lake", "warehouse", "exports"]


def test_a_lake_build_error_still_builds_the_warehouse_then_fails_the_run(batch_steps):
    batch_steps.failures["lake"] = LakeBuildError("current version not built for ['mines']")
    with pytest.raises(RuntimeError, match=r"build-lake: current version not built for \['mines'\]"):
        pipeline.batch(download=False)
    assert batch_steps.names == ["lake", "warehouse", "exports"]  # not retried: the same version would fail again


def test_any_other_lake_failure_is_retried_then_stops_the_run(batch_steps):
    batch_steps.failures["lake"] = OSError("disk full")
    with pytest.raises(OSError, match="disk full"):
        pipeline.batch(download=False)
    assert batch_steps.names == ["lake", "lake"]


def test_a_failed_warehouse_build_is_retried_then_skips_the_exports_and_fails_the_run(batch_steps):
    batch_steps.failures["warehouse"] = MemoryError("out of memory")
    with pytest.raises(RuntimeError, match="build-warehouse: .*out of memory"):
        pipeline.batch(download=False)
    assert batch_steps.names == ["lake", "warehouse", "warehouse"]


# The model flow -------------------------------------------------------------------------------------------------------


@pytest.fixture
def model_steps(monkeypatch, root):
    steps = Steps()

    def run_training(lake, out):
        steps("train", lake, out)
        return {"out": str(out), "selected": "logistic_regression", "test": {"logistic_regression": {"pr_auc": 0.2}}}

    def write_tables(lake, out):
        steps("analyze", lake, out)
        return {"ss_share_by_year": 27}

    monkeypatch.setattr("safestart_pipeline.ml.__main__.run_training", run_training)
    monkeypatch.setattr("safestart_pipeline.ml.analysis.write_tables", write_tables)
    return steps


def test_the_model_flow_trains_then_writes_the_analysis_from_the_lake(model_steps, root):
    summary = model_flow.model()
    lake = root / "lake" / "msha_ogd"
    assert model_steps.calls == [("train", lake, root / "models" / "accident_risk"),
                                 ("analyze", lake, root / "analysis")]
    assert summary["train-accident-risk"]["selected"] == "logistic_regression"
    assert summary["analyze-lake"] == {"ss_share_by_year": 27}


def test_a_failed_training_still_writes_the_analysis_then_fails_the_run(model_steps):
    model_steps.failures["train"] = ValueError("no labeled quarters")
    with pytest.raises(RuntimeError, match="train-accident-risk: .*no labeled quarters"):
        model_flow.model()
    assert model_steps.names == ["train", "train", "analyze"]


# The official flow ----------------------------------------------------------------------------------------------------


class FakeOfficialRun:
    """Stands in for official.run.OfficialRun: each source answers from `answers`, or raises its exception."""

    made: ClassVar[list["FakeOfficialRun"]] = []

    def __init__(self, root, client, *, until, budget=None, trigger="cli"):
        self.root, self.until, self.budget, self.trigger = root, until, budget, trigger
        self.run_id, self.asked, self.closed = "official-test", [], None
        self.answers: dict[str, object] = {}
        FakeOfficialRun.made.append(self)

    def source(self, name):
        self.asked.append(name)
        answer = self.answers.get(name, {"status": "ok", "note": f"{name} asked", "requests": 2,
                                         "stop_reason": "done", "details": {"changes": {"new": 1}}})
        if isinstance(answer, BaseException):
            raise answer
        return answer

    def close(self, stop_reason):
        self.closed = stop_reason


@pytest.fixture
def official_runs(monkeypatch, root):
    FakeOfficialRun.made = []
    monkeypatch.setattr(official_flow, "OfficialRun", FakeOfficialRun)
    monkeypatch.setattr(official_flow, "source_task", official_flow.source_task.with_options(retry_delay_seconds=0))
    monkeypatch.setattr(official_flow, "scheduled_start", lambda: datetime(2026, 11, 21, 16, 30, tzinfo=UTC))
    return FakeOfficialRun.made


def test_the_official_flow_asks_each_source_in_turn_until_the_deadline_of_its_scheduled_start(official_runs, root,
                                                                                               caplog, monkeypatch):
    caplog.set_level(logging.INFO, logger="test.official")
    monkeypatch.setattr(official_flow, "get_run_logger", lambda: logging.getLogger("test.official"))
    summary = official_flow.official()
    [run] = official_runs
    assert run.asked == ["msha", "ecfr", "federal_register", "arlweb", "osha"] and run.closed == "done"
    assert (run.root, run.until, run.trigger) == (root, datetime(2026, 11, 22, 4, 30, tzinfo=UTC), "flow")
    assert summary["until"] == "2026-11-22T04:30:00Z" and summary["sources"]["osha"]["status"] == "ok"
    assert any("osha: ok after 2 requests (done)" in record.getMessage() for record in caplog.records)


def test_a_failing_source_is_retried_once_the_others_still_run_and_the_flow_fails_at_the_end(official_runs,
                                                                                            monkeypatch):
    real_init = FakeOfficialRun.__init__

    def init(self, *args, **kwargs):
        real_init(self, *args, **kwargs)
        self.answers["arlweb"] = OSError("disk full")

    monkeypatch.setattr(FakeOfficialRun, "__init__", init)
    with pytest.raises(RuntimeError, match="arlweb: .*disk full"):
        official_flow.official(sources=["osha", "arlweb"], until="2026-11-22T01:00:00Z", budget=40)
    [run] = official_runs
    assert run.asked == ["arlweb", "arlweb", "osha"]  # retried once, then the next source
    assert (run.until, run.budget, run.closed) == (datetime(2026, 11, 22, 1, 0, tzinfo=UTC), 40, "errors")


# One writer of the data root ------------------------------------------------------------------------------------------


def test_a_batch_or_model_run_fails_at_once_while_another_holds_the_data_root(batch_steps, model_steps, root):
    with SiteLock(root / "state" / "pipeline.lock"):
        with pytest.raises(RuntimeError, match="one writer of the data root at a time"):
            pipeline.batch(download=False)
        with pytest.raises(RuntimeError, match="one writer of the data root at a time"):
            model_flow.model()
    assert batch_steps.calls == [] and model_steps.calls == []
    pipeline.batch(download=False)  # released with the run that held it
    assert batch_steps.names == ["lake", "warehouse", "exports"]


def test_a_batch_holds_the_data_roots_lock_while_its_steps_run(batch_steps, monkeypatch, root):
    held = []

    def build_lake(data_root):
        try:
            with SiteLock(data_root / "state" / "pipeline.lock"):
                held.append(False)
        except RuntimeError:
            held.append(True)
        return {}

    monkeypatch.setattr("safestart_pipeline.lake.msha_ogd.build_lake", build_lake)
    pipeline.batch(download=False)
    assert held == [True]


# Schedules ------------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(("schedule", "moment", "expected"), [
    (DAILY, datetime(2026, 11, 23, 13, 59, tzinfo=UTC), datetime(2026, 11, 23, 14, 0, tzinfo=UTC)),
    (DAILY, datetime(2026, 11, 23, 14, 0, tzinfo=UTC), datetime(2026, 11, 24, 14, 0, tzinfo=UTC)),
    (DAILY, datetime(2026, 11, 23, 9, 30, tzinfo=timezone(timedelta(hours=-5))),
     datetime(2026, 11, 24, 14, 0, tzinfo=UTC)),
    (WEEKLY, SATURDAY_NOON, datetime(2026, 11, 22, 15, 0, tzinfo=UTC)),
    (WEEKLY, datetime(2026, 11, 22, 15, 0, tzinfo=UTC), datetime(2026, 11, 29, 15, 0, tzinfo=UTC)),
    (WEEKLY, datetime(2026, 11, 23, 10, 0, tzinfo=UTC), datetime(2026, 11, 29, 15, 0, tzinfo=UTC)),
])
def test_the_next_run_is_the_first_scheduled_time_after_the_moment(schedule, moment, expected):
    assert schedule.next_after(moment) == expected


def test_prefect_schedules_the_deployments_at_the_times_the_local_scheduler_keeps():
    for job in flows_main.JOBS.values():
        dates = asyncio.run(CronSchedule(cron=job.schedule.cron, timezone="UTC").get_dates(n=12, start=SATURDAY_NOON))
        for hour in range(24 * 9):
            moment = SATURDAY_NOON + timedelta(hours=hour)
            assert job.schedule.next_after(moment) == min(date for date in dates if date > moment)


class Clock:
    def __init__(self, start: datetime):
        self.time = start
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        return self.time

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.time += timedelta(seconds=seconds)


class Stop(BaseException):
    """Ends a scheduler under test, as KeyboardInterrupt ends the real one (an Exception would be retried)."""


def scheduled_runs(minutes: dict[str, int], count: int) -> tuple[list[tuple[str, datetime]], Clock]:
    """The local scheduler's first `count` runs from Saturday noon of the flows named in `minutes`, each flow's run
    taking its minutes."""
    clock, runs = Clock(SATURDAY_NOON), []

    def run(name: str) -> None:
        runs.append((name, clock.now()))
        clock.time += timedelta(minutes=minutes[name])
        if len(runs) == count:
            raise Stop

    with pytest.raises(Stop):
        run_on_schedule({name: flows_main.JOBS[name].schedule for name in minutes}, run, now=clock.now,
                        sleep=clock.sleep)
    return runs, clock


def test_the_local_scheduler_runs_each_flow_at_its_time_and_one_that_came_due_meanwhile_next():
    runs, clock = scheduled_runs({"batch": 70, "model": 30, "official": OFFICIAL_MINUTES}, 6)
    assert runs == [("batch", datetime(2026, 11, 21, 14, 0, tzinfo=UTC)),
                    ("official", datetime(2026, 11, 21, 16, 30, tzinfo=UTC)),
                    ("batch", datetime(2026, 11, 22, 14, 0, tzinfo=UTC)),  # the official run ended at 04:30
                    ("model", datetime(2026, 11, 22, 15, 10, tzinfo=UTC)),  # due at 15:00, while the batch ran
                    ("official", datetime(2026, 11, 22, 16, 30, tzinfo=UTC)),
                    ("batch", datetime(2026, 11, 23, 14, 0, tzinfo=UTC))]
    assert max(clock.sleeps) <= MAX_SLEEP_SECONDS


def test_a_run_that_outlasts_its_flows_next_time_skips_that_time():
    runs, _ = scheduled_runs({"batch": 25 * 60, "model": 30}, 3)
    assert runs == [("batch", datetime(2026, 11, 21, 14, 0, tzinfo=UTC)),
                    ("model", datetime(2026, 11, 22, 15, 0, tzinfo=UTC)),
                    ("batch", datetime(2026, 11, 23, 14, 0, tzinfo=UTC))]


@pytest.mark.parametrize(("start", "expected"), [
    (datetime(2026, 11, 21, 16, 30, tzinfo=UTC), datetime(2026, 11, 22, 4, 30, tzinfo=UTC)),  # as scheduled
    (datetime(2026, 11, 21, 22, 0, tzinfo=UTC), datetime(2026, 11, 22, 4, 30, tzinfo=UTC)),
    (datetime(2026, 11, 22, 3, 0, tzinfo=UTC), datetime(2026, 11, 22, 4, 30, tzinfo=UTC)),
    (datetime(2026, 11, 22, 5, 0, tzinfo=UTC), datetime(2026, 11, 22, 13, 45, tzinfo=UTC)),  # by hand, by day
    (datetime(2026, 11, 22, 13, 50, tzinfo=UTC), datetime(2026, 11, 22, 13, 45, tzinfo=UTC)),  # already past
    (datetime(2026, 11, 22, 14, 30, tzinfo=UTC), datetime(2026, 11, 22, 14, 45, tzinfo=UTC)),  # Sunday: the model
    (datetime(2026, 11, 23, 14, 30, tzinfo=UTC), datetime(2026, 11, 24, 4, 30, tzinfo=UTC)),
])
def test_the_official_run_stops_before_safestarts_import_and_never_runs_into_another_flow(start, expected):
    assert official_run.deadline(start) == expected
    others = [job.schedule for name, job in flows_main.JOBS.items() if name != "official"]
    assert {(schedule.weekday, schedule.hour, schedule.minute) for schedule in others} == {
        (weekday, at.hour, at.minute) for weekday, at in official_run.KEEP_CLEAR}
    for schedule in others:  # one flow run at a time: the batch and the model never wait behind it
        assert official_run.deadline(start) < schedule.next_after(start)


def test_an_official_run_the_local_scheduler_starts_late_keeps_the_deadline_of_its_due_time(jobs, monkeypatch):
    """A batch that ran 25 hours leaves the official run due since yesterday 16:30: it ends at once, at the deadline
    of the time it was due, so the Sunday model does not wait 13 hours behind it."""
    monkeypatch.setattr(flows_main, "utc_now", lambda: datetime(2026, 11, 22, 15, 0, tzinfo=UTC))
    assert flows_main.run_flow("official", scheduled=True)["state"] == "completed"
    assert flows_main.run_flow("official")["state"] == "completed"  # by hand: the flow's own default
    assert jobs.official == [{"until": "2026-11-22T04:30:00Z"}, {}]


def batch_runs(failures: int, count: int) -> list[datetime]:
    """When the local scheduler starts the daily batch, the first `failures` runs raising before their flow starts (as
    when Prefect's local API server does not answer in time)."""
    clock, runs = Clock(SATURDAY_NOON), []

    def run(name: str) -> None:
        runs.append(clock.now())
        if len(runs) == count:
            raise Stop
        if len(runs) <= failures:
            raise RuntimeError("Timed out while attempting to connect to ephemeral Prefect API server.")

    with pytest.raises(Stop):
        run_on_schedule({"batch": DAILY}, run, now=clock.now, sleep=clock.sleep)
    return runs


def at(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 11, day, hour, minute, tzinfo=UTC)


def test_a_run_that_raises_is_tried_again_15_minutes_later_and_the_scheduler_goes_on(caplog):
    assert batch_runs(2, 4) == [at(21, 14), at(21, 14, 15), at(21, 14, 30), at(22, 14)]
    errors = [record for record in caplog.records if record.levelno == logging.ERROR]
    assert [record.getMessage() for record in errors] == [
        "batch: the run raised; trying again at 2026-11-21 14:15 UTC (retry 1 of 3)",
        "batch: the run raised; trying again at 2026-11-21 14:30 UTC (retry 2 of 3)"]
    assert all(record.exc_info[0] is RuntimeError for record in errors)  # each with its traceback


def test_a_run_that_keeps_raising_waits_for_its_next_time_after_three_retries():
    assert batch_runs(9, 6) == [at(21, 14), at(21, 14, 15), at(21, 14, 30), at(21, 14, 45), at(22, 14),
                                at(22, 14, 15)]


# `python -m safestart_pipeline.flows` ---------------------------------------------------------------------------------


def unexpected(*args, **kwargs):
    raise AssertionError(f"not expected: {args} {kwargs}")


def test_serve_registers_every_flow_with_the_prefect_api_when_one_is_set(monkeypatch):
    served = []
    monkeypatch.setenv("PREFECT_API_URL", "http://127.0.0.1:4200/api")
    monkeypatch.delenv("PREFECT_LOGGING_EXTRA_LOGGERS", raising=False)
    monkeypatch.setattr(flows_main, "serve", lambda *deployments, **options: served.append((deployments, options)))
    monkeypatch.setattr(flows_main, "run_on_schedule", unexpected)
    assert flows_main.main(["serve"]) == 0
    (deployments, options), = served
    assert options == {"limit": 1}  # one flow run at a time, as the local scheduler runs them
    assert [(deployment.flow_name, deployment.name, deployment.entrypoint,
             [(item.schedule.cron, item.schedule.timezone) for item in deployment.schedules])
            for deployment in deployments] == [
        ("safestart-batch", "daily", "safestart_pipeline.flows.pipeline.batch", [("0 14 * * *", "UTC")]),
        ("safestart-model", "weekly", "safestart_pipeline.flows.model.model", [("0 15 * * 0", "UTC")]),
        ("safestart-official", "daily", "safestart_pipeline.flows.official.official", [("30 16 * * *", "UTC")]),
    ]
    assert os.environ["PREFECT_LOGGING_EXTRA_LOGGERS"] == "safestart_pipeline"  # inherited by the runs' processes


def test_a_flow_lets_the_stages_progress_lines_through(batch_steps):
    logging.getLogger("safestart_pipeline").setLevel(logging.WARNING)  # as in a served run's own process
    pipeline.batch(download=False)
    assert logging.getLogger("safestart_pipeline").level == logging.INFO


def test_the_entry_points_own_lines_reach_the_log_when_it_runs_as_a_module(monkeypatch, caplog):
    """`python -m safestart_pipeline.flows` runs __main__.py under the name "__main__". Its own lines ("batch run
    completed", "stopped") must still reach the log, where Prefect's logging configuration leaves the root logger at
    WARNING."""
    def stopped(*args, **kwargs):
        raise KeyboardInterrupt  # the container is stopped

    monkeypatch.setattr("safestart_pipeline.flows.schedule.run_on_schedule", stopped)
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    monkeypatch.setattr(sys, "argv", ["python -m safestart_pipeline.flows", "serve"])
    monkeypatch.delitem(sys.modules, "safestart_pipeline.flows.__main__")  # run afresh, as a new process would
    root, level = logging.getLogger(), logging.getLogger().level
    root.setLevel(logging.WARNING)
    try:
        with pytest.raises(SystemExit) as ended:
            runpy.run_module("safestart_pipeline.flows", run_name="__main__")  # as `python -m` runs it
    finally:
        root.setLevel(level)
    assert ended.value.code == 0
    assert [record.getMessage() for record in caplog.records if record.levelno == logging.INFO] == ["stopped"]


def test_serve_keeps_the_schedule_in_this_process_without_a_prefect_api(monkeypatch):
    clock, runs = Clock(datetime(2026, 11, 23, 13, 59, 30, tzinfo=UTC)), []

    def run_flow(name, **options):
        runs.append((name, clock.now(), options))
        raise KeyboardInterrupt  # the container is stopped

    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    monkeypatch.setattr(flows_main, "serve", unexpected)
    monkeypatch.setattr(flows_main, "run_on_schedule", partial(run_on_schedule, now=clock.now, sleep=clock.sleep))
    monkeypatch.setattr(flows_main, "run_flow", run_flow)
    assert flows_main.main(["serve"]) == 0
    assert runs == [("batch", datetime(2026, 11, 23, 14, 0, tzinfo=UTC), {"scheduled": True})]
    assert clock.sleeps == [30.0]


class Calls(list):
    """The batch's download choices; `official` holds the parameters the official flow was given."""

    official: list[dict]


@pytest.fixture
def jobs(monkeypatch):
    """Stand-ins for the three flows: the batch records its download choice, the model fails, the official flow
    records its parameters."""
    calls = Calls()
    calls.official = []

    @flow(name="safestart-batch")
    def batch(download: bool = True):
        calls.append(download)
        return {"lake": {"mines": {"rows": 3}}}

    @flow(name="safestart-model")
    def model():
        raise RuntimeError("train-accident-risk: no labeled quarters")

    @flow(name="safestart-official")
    def official_files(until: str | None = None):
        calls.official.append({"until": until} if until else {})
        return {"sources": {}}

    monkeypatch.setattr(flows_main, "JOBS", {"batch": flows_main.Job(batch, "daily", DAILY),
                                             "model": flows_main.Job(model, "weekly", WEEKLY),
                                             "official": flows_main.Job(official_files, "daily",
                                                                        Schedule(hour=16, minute=30))})
    return calls


def test_run_runs_the_batch_now_and_prints_its_outcome(jobs, capsys):
    assert flows_main.main(["run", "--no-download"]) == 0
    assert jobs == [False]
    assert json.loads(capsys.readouterr().out) == {
        "batch": {"state": "completed", "result": {"lake": {"mines": {"rows": 3}}}}}


def test_a_failed_flow_is_reported_and_the_next_one_still_runs(jobs, capsys):
    assert flows_main.run_once(["model", "batch"]) == 1
    assert jobs == [True]
    outcomes = json.loads(capsys.readouterr().out)
    assert outcomes["model"]["state"] == "failed"
    assert "RuntimeError: train-accident-risk: no labeled quarters" in outcomes["model"]["error"]
    assert outcomes["batch"]["state"] == "completed"


def test_no_download_is_refused_for_the_model_flow(jobs):
    with pytest.raises(SystemExit):
        flows_main.main(["run", "model", "--no-download"])
    assert jobs == []
