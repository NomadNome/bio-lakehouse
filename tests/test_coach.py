"""Tests for the adaptive training coach (guardrails + scoring)."""

from datetime import date

import pytest

from insights_engine.coach.planner import CoachConfig, TrainingCoach
from insights_engine.insights.what_if import (
    DayProjection,
    MultiDayResult,
    WhatIfSimulator,
)


class FakeSimulator:
    """Deterministic stand-in for WhatIfSimulator (no Athena)."""

    def __init__(self, ctl=40.0, atl=45.0):
        self._ctl = ctl
        self._atl = atl

    def load_historical_models(self):
        return {"tss_history": {"ctl": self._ctl, "atl": self._atl}}

    def simulate_multi_day(self, plans):
        projections = []
        ctl, atl = self._ctl, self._atl
        for p in sorted(plans, key=lambda x: x.day_offset):
            tss = TrainingCoach._est_tss(p.workout_type, p.workout_intensity)
            ctl = ctl + (tss - ctl) * (2.0 / 43)
            atl = atl + (tss - atl) * (2.0 / 8)
            projections.append(
                DayProjection(
                    day_offset=p.day_offset,
                    date_label=f"day{p.day_offset}",
                    predicted_readiness=80.0,
                    energy_state="moderate",
                    overtraining_risk="low",
                    estimated_tss=tss,
                    projected_ctl=round(ctl, 1),
                    projected_atl=round(atl, 1),
                    projected_tsb=round(ctl - atl, 1),
                )
            )
        return MultiDayResult(projections=projections, starting_ctl=self._ctl,
                              starting_atl=self._atl)


@pytest.fixture
def coach():
    return TrainingCoach(FakeSimulator())


class TestGuardrails:
    def test_always_at_least_one_rest_day(self, coach):
        cfg = CoachConfig(available_weekdays=list(range(7)))
        plan = coach.prescribe(cfg)
        assert any(d.intensity == "none" for d in plan.days)

    def test_max_consecutive_training_days(self, coach):
        cfg = CoachConfig(available_weekdays=list(range(7)), max_consecutive=2)
        plan = coach.prescribe(cfg)
        streak = 0
        for d in plan.days:
            streak = streak + 1 if d.intensity != "none" else 0
            assert streak <= 2

    def test_no_high_days_in_recovery_goal(self, coach):
        cfg = CoachConfig(goal="recover", available_weekdays=list(range(7)))
        plan = coach.prescribe(cfg)
        assert all(d.intensity != "high" for d in plan.days)

    def test_hard_day_cap(self, coach):
        cfg = CoachConfig(available_weekdays=list(range(7)), max_hard_days=2)
        plan = coach.prescribe(cfg)
        assert sum(1 for d in plan.days if d.intensity == "high") <= 2

    def test_unavailable_days_are_rest(self, coach):
        # Only Monday(0) and Thursday(3) available
        cfg = CoachConfig(available_weekdays=[0, 3])
        plan = coach.prescribe(cfg)
        for d in plan.days:
            wd = (date.today().weekday() + d.day_offset) % 7
            if wd not in (0, 3):
                assert d.intensity == "none", f"{d.weekday} should be rest"

    def test_no_back_to_back_high(self, coach):
        cfg = CoachConfig(available_weekdays=list(range(7)), max_consecutive=3)
        plan = coach.prescribe(cfg)
        for a, b in zip(plan.days, plan.days[1:]):
            assert not (a.intensity == "high" and b.intensity == "high")


class TestPlanShape:
    def test_covers_seven_days(self, coach):
        plan = coach.prescribe(CoachConfig())
        assert len(plan.days) == 7
        assert [d.day_offset for d in plan.days] == list(range(1, 8))

    def test_weekly_tss_matches_day_sum(self, coach):
        plan = coach.prescribe(CoachConfig())
        assert plan.weekly_tss == round(sum(d.target_tss for d in plan.days), 0)

    def test_disciplines_restricted_to_config(self, coach):
        cfg = CoachConfig(disciplines=["strength"], available_weekdays=list(range(7)))
        plan = coach.prescribe(cfg)
        used = {d.discipline for d in plan.days if d.intensity != "none"}
        assert used <= {"strength"}

    def test_recovery_target_below_maintain(self, coach):
        rec = coach.prescribe(CoachConfig(goal="recover"))
        keep = coach.prescribe(CoachConfig(goal="maintain"))
        assert rec.target_weekly_tss < keep.target_weekly_tss

    def test_every_day_has_why(self, coach):
        plan = coach.prescribe(CoachConfig())
        assert all(d.why for d in plan.days)

    def test_alternatives_reported(self, coach):
        plan = coach.prescribe(CoachConfig())
        assert len(plan.alternatives) == 2


class TestSavePlan:
    def test_save_plan_local(self, coach, tmp_path):
        import json

        plan = coach.prescribe(CoachConfig())
        out = tmp_path / "plan.json"
        payload = coach.save_plan(plan, local_path=str(out))
        saved = json.loads(out.read_text())
        assert saved == payload
        assert len(saved["days"]) == 7
        assert saved["generated_at"] == date.today().isoformat()
        assert all("intensity" in d and "date" in d for d in saved["days"])
