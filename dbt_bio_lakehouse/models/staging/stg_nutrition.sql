SELECT
    date,
    calories AS daily_calories,
    protein_g,
    carbohydrates_g AS carbs_g,
    fat_g,
    fiber_g,
    sugar_g,
    sodium_mg,
    cholesterol_mg,
    protein_pct,
    carb_pct,
    fat_pct,
    meal_count,
    meals_logged
FROM {{ source('bio_silver', 'mfp_daily_nutrition') }}
