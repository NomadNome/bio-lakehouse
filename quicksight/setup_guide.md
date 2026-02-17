# QuickSight Dashboard Setup Guide

## Prerequisites

1. Gold layer populated (readiness aggregator has run)
2. Gold Glue Crawler has run (tables visible in Athena)
3. QuickSight Enterprise edition enabled in your AWS account

## Step 1: Enable QuickSight Athena Access

1. Go to **QuickSight** > **Manage QuickSight** > **Security & permissions**
2. Under **QuickSight access to AWS services**, click **Manage**
3. Enable:
   - **Amazon Athena** (check the box)
   - **Amazon S3** — select these buckets:
     - `bio-lakehouse-gold-{AccountId}`
     - `bio-lakehouse-athena-results-{AccountId}`

## Step 2: Create Athena Dataset

1. In QuickSight, go to **Datasets** > **New dataset**
2. Select **Athena** as the data source
3. Name: `bio-gold-readiness-performance`
4. Select workgroup: `primary`
5. Database: `bio_gold`
6. Table: `daily_readiness_performance`
7. Choose **Import to SPICE** for faster queries
8. Click **Edit/Preview data** to verify columns look correct
9. Save & publish

### Alternative: Use the Dashboard 30-Day View

To get rolling averages built-in:
1. Instead of selecting a table, choose **Use custom SQL**
2. Paste: `SELECT * FROM bio_gold.dashboard_30day`
3. This gives you pre-computed 7-day and 30-day rolling averages

## Step 3: Build Dashboards

### Dashboard 1: Recovery vs. Performance Trends

**Visual 1 — Dual-Axis Line Chart:**
- X-axis: `date`
- Left Y-axis: `readiness_score` (line), `sleep_score` (line)
- Right Y-axis: `total_output_kj` (bar)
- Filter: Last 30 days

**Visual 2 — KPI Cards:**
- Average readiness score (last 7 days)
- Average sleep score (last 7 days)
- Total workouts this week
- Total output (kJ) this week

**Visual 3 — Scatter Plot:**
- X-axis: `readiness_score`
- Y-axis: `total_output_kj`
- Size: `workout_count`
- Color: `had_workout`

### Dashboard 2: Workout Type Optimizer

**Visual 1 — Stacked Bar Chart:**
- X-axis: `date` (weekly aggregation)
- Values: `total_output_kj`
- Group by: `workout_categories`

**Visual 2 — Pivot Table:**
- Rows: `disciplines`
- Values: AVG(`avg_watts`), SUM(`total_output_kj`), AVG(`readiness_score`)
- Sort by total output descending

**Visual 3 — Heat Map:**
- Rows: Day of week (calculated field: `extract('DOW', parseDate(date, 'yyyy-MM-dd'))`)
- Columns: `recommended_intensity` (from workout_recommendations view)
- Values: Count

### Dashboard 3: Long-Term Fitness Trends

**Visual 1 — Line Chart (Monthly Trends):**
- X-axis: `date` (monthly aggregation)
- Y-axis: `readiness_30day_avg`, `sleep_30day_avg`

**Visual 2 — Bar Chart (Monthly Volume):**
- X-axis: Month
- Y-axis: SUM(`total_output_kj`), COUNT(`workout_count`)

**Visual 3 — Calculated Trend:**
- Create calculated field: `periodOverPeriodDifference(sum(total_output_kj), date, MONTH, 1)`
- Visualize month-over-month output change

## Step 4: Schedule SPICE Refresh

1. Go to **Datasets** > select your dataset
2. Click **Schedule refresh**
3. Set: Daily at 3:00 AM UTC (after the 2:00 AM Gold aggregator runs)
4. Timezone: UTC

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
```
