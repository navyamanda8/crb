WITH revenue_changes AS (
    SELECT
        customer_id,
        payment_month,
        SUM(revenue) AS revenue
    FROM {{ ref('stg_transactions') }}
    GROUP BY customer_id, payment_month
),

previous_revenue AS (
    SELECT
        customer_id,
        payment_month,
        revenue,
        LAG(revenue) OVER (
            PARTITION BY customer_id 
            ORDER BY payment_month
        ) AS prev_revenue
    FROM revenue_changes
),

monthly_revenue AS (
    SELECT
        payment_month,
        SUM(revenue) AS total_revenue,
        SUM(prev_revenue) AS starting_revenue,
        SUM(
            CASE 
                WHEN revenue > prev_revenue THEN revenue - prev_revenue 
                ELSE 0 
            END
        ) AS expansion_revenue,
        SUM(
            CASE 
                WHEN revenue < prev_revenue THEN prev_revenue - revenue 
                ELSE 0 
            END
        ) AS contraction_revenue
    FROM previous_revenue
    WHERE prev_revenue IS NOT NULL
    GROUP BY payment_month
)

SELECT
    payment_month,
    total_revenue,
    starting_revenue,
    expansion_revenue,
    contraction_revenue,
    ROUND((starting_revenue + expansion_revenue - contraction_revenue) / starting_revenue, 4) AS NRR,
    ROUND((starting_revenue - contraction_revenue) / starting_revenue, 4) AS GRR
FROM monthly_revenue
ORDER BY payment_month
