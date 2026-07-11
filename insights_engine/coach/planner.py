"""
Bio Insights Engine - Adaptive Training Coach

Generates a periodized 7-day training plan from the athlete's current
training load (CTL/ATL/TSB) and readiness model, instead of asking the
user to hand-build scenarios in the What-If planner.

How it works:
  1. Read current fitness (CTL), fatigue (ATL), and form (TSB) from
     training_load_daily via the WhatIfSimulator's cached models.
  2. Pick a weekly TSS target from the goal (build / maintain / recover).
  3. Construct three candidate weekly patterns (aggressive / balanced /
     conservative) that respect hard periodization guardrails:
       - at least one full rest day
       - hard days capped and maximally spaced
       - no more than `max_consecutive` training days in a row
       - disciplines alternate
  4. Score every candidate through simulate_multi_day() — the same
     cascading projection the What-If planner uses — and prescribe the
     best one, with the runners-up kept for transparency.

Everything is deterministic and explainable: each day carries a "why".
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, timedelta

from insights_engine.config import WORKOUT_TSS_ESTIMATES
from insights_engine.insights.what_if import DayPlan, MultiDayResult, WhatIfSimulator

WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

# Weekly TSS multiplier vs current CTL, per goal
GOAL_FACTORS = {
    "build": 1.15,      # ramp fitness (~+2-4 CTL/week at typical loads)
    "maintain": 1.0,    # hold fitness
    "recover": 0.6,     # deload week
}

# Beginner floor: with near-zero CTL, prescribe from session estimates
MIN_WEEKLY_TSS = 120.0


@dataclass
class CoachConfig:
    goal: str = "build"                          # build | maintain | recover
    available_weekdays: list = field(default_factory=lambda: list(range(7)))
    disciplines: list = field(default_factory=lambda: ["cycling", "strength"])
    expected_sleep: int = 80
    max_hard_days: int = 2
    max_consecutive: int = 2


@dataclass
class CoachDay:
    day_offset: int
    date_label: str
    weekday: str
    discipline: str          # cycling | strength | cycling_and_strength | rest
    intensity: str           # none | low | moderate | high
    target_tss: float
    predicted_readiness: float
    projected_tsb: float
    energy_state: str
    why: str


@dataclass
class CoachPlan:
    days: list = field(default_factory=list)
    strategy: str = ""
    weekly_tss: float = 0.0
    target_weekly_tss: float = 0.0
    starting_ctl: float = 0.0
    ending_ctl: float = 0.0
    starting_tsb: float = 0.0
    rationale: list = field(default_factory=list)
    result: MultiDayResult | None = None
    alternatives: list = field(default_factory=list)   # (strategy, weekly_tss, score)


class TrainingCoach:
    """Prescribes a 7-day plan by generating and scoring candidate weeks."""

    def __init__(self, simulator: WhatIfSimulator):
        self.sim = simulator

    # ── public API ───────────────────────────────────────────────────────

    def prescribe(self, config: CoachConfig) -> CoachPlan:
        models = self.sim.load_historical_models()
        ctl = models["tss_history"]["ctl"]
        atl = models["tss_history"]["atl"]
        tsb = ctl - atl

        target = max(MIN_WEEKLY_TSS, 7 * ctl * GOAL_FACTORS.get(config.goal, 1.0))

        candidates = []
        for strategy, counts in self._intensity_mixes(config, target):
            assignment = self._assign_days(config, counts, tsb)
            plans = self._to_day_plans(assignment, config)
            mdr = self.sim.simulate_multi_day(plans)
            score = self._score(mdr, target)
            candidates.append((strategy, assignment, mdr, score))

        candidates.sort(key=lambda c: c[3], reverse=True)
        strategy, assignment, mdr, _ = candidates[0]

        days = self._build_days(assignment, mdr, config, tsb)
        weekly_tss = sum(d.target_tss for d in days)

        plan = CoachPlan(
            days=days,
            strategy=strategy,
            weekly_tss=round(weekly_tss, 0),
            target_weekly_tss=round(target, 0),
            starting_ctl=ctl,
            ending_ctl=mdr.projections[-1].projected_ctl if mdr.projections else ctl,
            starting_tsb=round(tsb, 1),
            result=mdr,
            rationale=self._rationale(config, ctl, atl, tsb, target, strategy),
            alternatives=[
                (s, round(sum(self._est_tss(d, i) for _, d, i in a), 0), round(sc, 1))
                for s, a, _, sc in candidates[1:]
            ],
        )
        return plan

    def save_plan(self, plan: CoachPlan, s3_bucket: str = "", local_path: str = "") -> dict:
        """Persist the plan so the morning briefing can surface today's session."""
        payload = {
            "generated_at": date.today().isoformat(),
            "strategy": plan.strategy,
            "goal_weekly_tss": plan.target_weekly_tss,
            "days": [
                {
                    "date": (date.today() + timedelta(days=d.day_offset)).isoformat(),
                    "weekday": d.weekday,
                    "discipline": d.discipline,
                    "intensity": d.intensity,
                    "target_tss": d.target_tss,
                    "predicted_readiness": d.predicted_readiness,
                    "why": d.why,
                }
                for d in plan.days
            ],
        }
        body = json.dumps(payload, indent=2)
        if local_path:
            with open(local_path, "w") as f:
                f.write(body)
        if s3_bucket:
            import boto3

            boto3.client("s3").put_object(
                Bucket=s3_bucket,
                Key="coach/current_plan.json",
                Body=body.encode("utf-8"),
                ContentType="application/json",
            )
        return payload

    # ── candidate generation ─────────────────────────────────────────────

    @staticmethod
    def _est_tss(discipline: str, intensity: str) -> float:
        if discipline == "rest" or intensity == "none":
            return 0.0
        return float(WORKOUT_TSS_ESTIMATES.get(discipline, {}).get(intensity, 0.0))

    def _intensity_mixes(self, config: CoachConfig, target: float):
        """Yield (strategy, (n_high, n_moderate, n_low)) candidates."""
        k = len(config.available_weekdays)
        h_max = min(config.max_hard_days, k, 2)

        mixes = [
            ("aggressive", (h_max, max(0, min(k - h_max - 1, 3)), 1)),
            ("balanced", (min(1, h_max), max(0, min(k - 2, 3)), 1)),
            ("conservative", (0, max(1, min(k - 1, 2)), max(1, min(k - 2, 2)))),
        ]
        if config.goal == "recover":
            # A deload week never includes high-intensity work
            mixes = [
                ("recovery-moderate", (0, min(2, k), max(0, min(k - 2, 2)))),
                ("recovery-light", (0, min(1, k), max(1, min(k - 1, 3)))),
                ("recovery-minimal", (0, 0, min(3, k))),
            ]
        return mixes

    def _assign_days(self, config: CoachConfig, counts: tuple, tsb: float):
        """Place intensities onto weekday offsets with spacing guardrails.

        Returns a list of (day_offset, discipline, intensity) for all 7 days.
        """
        n_high, n_mod, n_low = counts
        today_wd = date.today().weekday()
        available = []
        for offset in range(1, 8):
            wd = (today_wd + offset) % 7
            if wd in config.available_weekdays:
                available.append(offset)

        # Hard days: maximally spaced across available slots. If starting
        # form is deeply negative, push the first hard day later in the week.
        hard_slots = []
        if n_high > 0 and available:
            pool = available if tsb > -15 else available[1:] or available
            if n_high == 1:
                hard_slots = [pool[len(pool) // 3]]
            else:
                hard_slots = [pool[0], pool[-1]] if len(pool) > 1 else [pool[0]]

        assignment = {}
        consecutive = 0
        disc_cycle = 0
        disciplines = config.disciplines or ["cycling"]
        remaining = {"high": n_high, "moderate": n_mod, "low": n_low}

        for offset in range(1, 8):
            wd = (today_wd + offset) % 7
            if wd not in config.available_weekdays:
                assignment[offset] = ("rest", "none")
                consecutive = 0
                continue
            if consecutive >= config.max_consecutive:
                assignment[offset] = ("rest", "none")
                consecutive = 0
                continue

            prev = assignment.get(offset - 1, ("rest", "none"))
            if offset in hard_slots and remaining["high"] > 0 and prev[1] != "high":
                intensity = "high"
            elif remaining["moderate"] > 0:
                intensity = "moderate"
            elif remaining["low"] > 0:
                intensity = "low"
            else:
                assignment[offset] = ("rest", "none")
                consecutive = 0
                continue

            remaining[intensity] -= 1
            discipline = disciplines[disc_cycle % len(disciplines)]
            # Hard sessions go to cycling when possible (bigger, measurable TSS)
            if intensity == "high" and "cycling" in disciplines:
                discipline = "cycling"
            assignment[offset] = (discipline, intensity)
            disc_cycle += 1
            consecutive += 1

        # Guarantee at least one full rest day
        if all(v[1] != "none" for v in assignment.values()):
            mid = 4
            assignment[mid] = ("rest", "none")

        return [(o, d, i) for o, (d, i) in sorted(assignment.items())]

    def _to_day_plans(self, assignment, config: CoachConfig) -> list:
        return [
            DayPlan(
                day_offset=offset,
                sleep_score=config.expected_sleep,
                workout_type=discipline,
                workout_intensity=intensity,
            )
            for offset, discipline, intensity in assignment
        ]

    # ── scoring & explanation ────────────────────────────────────────────

    @staticmethod
    def _score(mdr: MultiDayResult, target: float) -> float:
        """Higher is better: hit the TSS target, keep readiness, avoid risk."""
        if not mdr.projections:
            return -999.0
        weekly = sum(p.estimated_tss for p in mdr.projections)
        tss_err = abs(weekly - target) / max(target, 1.0)
        mean_readiness = sum(p.predicted_readiness for p in mdr.projections) / len(
            mdr.projections
        )
        risk_days = sum(1 for p in mdr.projections if p.overtraining_risk == "high")
        low_days = sum(
            1 for p in mdr.projections if p.energy_state in ("low", "recovery_needed")
        )
        return mean_readiness - 40 * tss_err - 12 * risk_days - 4 * low_days

    def _build_days(self, assignment, mdr: MultiDayResult, config, start_tsb):
        proj_by_offset = {p.day_offset: p for p in mdr.projections}
        days = []
        for offset, discipline, intensity in assignment:
            p = proj_by_offset.get(offset)
            wd = WEEKDAYS[(date.today().weekday() + offset) % 7]
            days.append(
                CoachDay(
                    day_offset=offset,
                    date_label=p.date_label if p else "",
                    weekday=wd,
                    discipline=discipline,
                    intensity=intensity,
                    target_tss=self._est_tss(discipline, intensity),
                    predicted_readiness=p.predicted_readiness if p else 0.0,
                    projected_tsb=p.projected_tsb if p else 0.0,
                    energy_state=p.energy_state if p else "moderate",
                    why=self._why(discipline, intensity, p, config, start_tsb),
                )
            )
        return days

    @staticmethod
    def _why(discipline, intensity, p, config, start_tsb) -> str:
        if intensity == "none":
            wd_available = p is not None and (
                (date.today().weekday() + p.day_offset) % 7 in config.available_weekdays
            )
            if not wd_available:
                return "Not one of your training days."
            return "Rest keeps the consecutive-day streak in check and lets form rebuild."
        if intensity == "high":
            base = f"Hard {discipline} scheduled where projected form (TSB) can absorb it"
            if p:
                base += f" — TSB {p.projected_tsb:+.0f}, predicted readiness {p.predicted_readiness:.0f}"
            return base + "."
        if intensity == "moderate":
            return f"Moderate {discipline} builds load without spiking fatigue."
        return f"Light {discipline} adds volume while staying fully recoverable."

    @staticmethod
    def _rationale(config, ctl, atl, tsb, target, strategy) -> list:
        lines = [
            f"Current fitness (CTL) {ctl:.0f}, fatigue (ATL) {atl:.0f}, form (TSB) {tsb:+.0f}.",
            f"Goal '{config.goal}' sets a weekly target of ~{target:.0f} TSS "
            f"({GOAL_FACTORS.get(config.goal, 1.0):.0%} of 7x CTL"
            + (", floored for low training history)." if target == MIN_WEEKLY_TSS else ")."),
            f"'{strategy.title()}' pattern won the simulation: best blend of hitting the "
            "TSS target, projected readiness, and zero high-risk days.",
        ]
        if tsb < -15:
            lines.append(
                f"Form is deep in the hole (TSB {tsb:+.0f}) — the first hard day is "
                "deferred until fatigue clears."
            )
        return lines
