# QuickSight Dashboard Setup Guide

## Prerequisites

1. Gold layer populated (readiness aggregator has run)
2. Gold Glue Crawler has run (tables visible in Athena)
3. QuickSight Standard edition enabled in your AWS account
4. SPICE datasets already created via CLI (7 datasets total)

## Available Datasets

| Dataset | Rows | Purpose |
|---------|------|---------|
| Bio Daily Readiness & Performance | 612 | Main Gold table — all metrics joined |
| Bio Dashboard 30-Day Rolling | 612 | 7-day and 30-day rolling averages |
| Bio Workout Recommendations | 612 | Daily intensity recommendations |
| Bio Energy State | 84 | 125% energy zone classification |
| Bio Workout Type Optimization | 8 | Historical output by readiness bucket x workout type |
| Bio Weekly Trends | 13 | Week-over-week progression with trend indicators |
| Bio Overtraining Risk Monitor | 84 | Overtraining risk flags and guidance |

## Available Athena Views

| View | What it answers |
|------|----------------|
| `bio_gold.energy_state` | What's my energy zone today? Peak/High/Moderate/Low/Recovery |
| `bio_gold.workout_type_optimization` | Which workout type gives best output at my readiness level? |
| `bio_gold.sleep_performance_prediction` | How does last night's sleep predict tomorrow's performance? |
| `bio_gold.readiness_performance_correlation` | Do readiness and output actually correlate? Statistical analysis. |
| `bio_gold.weekly_trends` | Am I improving, declining, or overreaching week over week? |
| `bio_gold.overtraining_risk` | Am I pushing too hard? Flags high/moderate/low risk days. |
| `bio_gold.dashboard_30day` | Pre-computed 7-day and 30-day rolling averages |
| `bio_gold.workout_recommendations` | Daily workout intensity recommendations with guidance text |

---

## Dashboard 1: Recovery vs Performance Trends

**Dataset:** Bio Daily Readiness & Performance

1. Click **New Analysis** → select **Bio Daily Readiness & Performance** → Create Analysis

2. **KPI Cards** (4 across the top):
   - Drag `readiness_score` → change aggregation to Average
   - Drag `sleep_score` → change aggregation to Average
   - Drag `total_output_kj` → change aggregation to Average
   - Drag `date` → change aggregation to Count (total days tracked)

3. **Line Chart — Readiness & Sleep Over Time:**
   - X-axis: `date`
   - Values: `readiness_score` (avg), `sleep_score` (avg)
   - Granularity: Day

4. **Bar Chart — Monthly Output:**
   - X-axis: `date` (set granularity to **Month**)
   - Value: `total_output_kj` (sum)

5. **Scatter Plot — Readiness vs Output:**
   - X-axis: `readiness_score`
   - Y-axis: `total_output_kj`
   - Color: `month`

---

## Dashboard 2: Energy State & Optimization

**Dataset:** Bio Energy State

1. **New Analysis** → select **Bio Energy State**

2. **Donut/Pie Chart — Energy State Distribution:**
   - Group by: `energy_state`
   - Value: count
   - Shows how many days you spend in each zone (peak/high/moderate/low/recovery)

3. **Table — Today's Guidance:**
   - Columns: `date`, `energy_state`, `readiness_score`, `sleep_score`, `hrv_balance`, `guidance`
   - Sort by `date` descending
   - This is your daily "what should I do" reference

4. **Bar Chart — Output by Energy State:**
   - X-axis: `energy_state`
   - Value: `total_output_kj` (avg)
   - Shows which energy zones produce the most output

5. **Line Chart — Readiness Delta Trend:**
   - X-axis: `date`
   - Values: `readiness_delta`, `sleep_delta`
   - Shows day-over-day momentum (positive = improving, negative = declining)

---

## Dashboard 3: Workout Type Optimizer

**Dataset:** Bio Workout Type Optimization

1. **New Analysis** → select **Bio Workout Type Optimization**

2. **Pivot Table / Heat Map (core optimization table):**
   - Rows: `readiness_bucket`
   - Columns: `workout_type`
   - Values: `avg_output_kj`
   - This tells you: "When my readiness is X, which workout gives the best output?"

3. **Grouped Bar Chart:**
   - X-axis: `workout_type`
   - Value: `avg_output_kj`
   - Group/Color: `readiness_bucket`

4. **Table — Full Detail:**
   - All columns: `readiness_bucket`, `workout_type`, `sample_days`, `avg_output_kj`, `avg_watts`, `avg_calories`, `avg_duration_min`

---

## Dashboard 4: Weekly Trends & Overtraining

### Sheet 1 — Weekly Trends
**Dataset:** Bio Weekly Trends

1. **Line Chart — Weekly Readiness + Output:**
   - X-axis: `week_start`
   - Values: `avg_readiness` (line 1), `weekly_output_kj` (line 2, secondary axis)

2. **Table — Week-over-Week:**
   - Columns: `week_start`, `avg_readiness`, `readiness_change`, `weekly_output_kj`, `output_change`, `workout_days`, `trend`
   - The `trend` column shows: improving / declining / overreaching / recovering

3. **Bar Chart — Workout Days per Week:**
   - X-axis: `week_start`
   - Value: `workout_days`

### Sheet 2 — Overtraining Monitor
**Dataset:** Bio Overtraining Risk Monitor (add via pencil icon → Add dataset)

1. **Donut Chart — Risk Distribution:**
   - Group by: `overtraining_risk`
   - Value: count

2. **Table — Risk Detail:**
   - Columns: `date`, `readiness_score`, `hrv_balance`, `workouts_last_3_days`, `overtraining_risk`, `risk_guidance`
   - Sort by `date` descending
   - Add filter: `overtraining_risk` = `high_risk` or `moderate_risk`

---

## General QuickSight Tips

- **Add filters:** Click "Filter" in left panel → add `date` filter → set to "Last 30 days" or "Last 90 days"
- **Rename visuals:** Double-click any chart title to customize
- **Secondary Y-axis:** Click a line chart → Format → select the second measure → "Show on secondary axis"
- **Conditional formatting:** On tables, right-click a column → Conditional formatting → set red/yellow/green for risk levels or energy states
- **Multiple sheets:** Click the "+" tab at the bottom to add sheets within one analysis

## Calculated Fields Reference

Useful calculated fields to create in QuickSight:

```
# Day of Week
dayOfWeek = extract('DOW', parseDate({date}, 'yyyy-MM-dd'))

# Week Number
weekNumber = extract('WK', parseDate({date}, 'yyyy-MM-dd'))

# Output Efficiency (kJ per minute)
outputEfficiency = {total_output_kj} / nullIf({total_workout_minutes}, 0)

# Recovery Quality Tier
recoveryTier = ifelse(
    {readiness_score} >= 85, 'Excellent',
    {readiness_score} >= 70, 'Good',
    {readiness_score} >= 50, 'Fair',
    'Poor'
)

# 125% Energy Flag
isPeakEnergy = ifelse({energy_state} = 'peak', 'YES', 'NO')
```

## Schedule SPICE Refresh

1. Go to **Datasets** > select your dataset
2. Click **Schedule refresh**
3. Set: Daily at 3:00 AM UTC (after the 2:00 AM Gold aggregator runs)
4. Timezone: UTC
5. Repeat for each dataset you want auto-refreshed
