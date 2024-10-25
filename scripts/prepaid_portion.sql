-- prepaid
SELECT
    d. *,
    C."name",
    C .report_region,
    C .dealer_class
FROM
    (
        SELECT
            b.payout_date,
            business,
            LOB,
            b.incentive_type,
            b.dealer_code,
            substring(cast(b.activation_month AS text), 5, 2) AS act_month,
            substring(cast(b.activation_month AS text), 1, 4) AS act_year,
            COUNT(b. *) AS COUNT,
            SUM(b.amount) AS incentive,
            cast(date_part('year', b.payout_date) AS text) AS payout_year,
            cast(date_part('month', b.payout_date) AS text) AS payout_month,
            'edis' AS simulator_type,
            CASE
                WHEN post2pre IS NOT NULL THEN 'Y'
                ELSE 'N'
            END AS p2p,
            CASE
                WHEN mnp IS NOT NULL THEN 'Y'
                ELSE 'N'
            END AS MAP,
            rate_plan,
            evaluation_month,
            appr_userid
        FROM
            sdbpayment.pymt_prepaid_details b
            INNER JOIN (
                SELECT
                    DISTINCT batch_id,
                    payout_date,
                    original_payout_date,
                    payout_status
                FROM
                    sdbpayment.pymt_prepaid_payment
                WHERE
                    payout_date >= '2024-08-01'
                    AND payout_date < '2024-09-01' --- CHANGE THIS ---
                    AND original_payout_date < '2024-09-01' --- CHANGE THIS ---
                    AND payout_status IN ('SENT-SAP', 'SUCCESS-ERELOAD', 'SUCCESS-SAP')
            ) A ON A .batch_id = b.batch_id
            AND A .payout_date = b.payout_date
            AND A .original_payout_date = b.original_payout_date
            AND A .payout_status = b.payout_status
        GROUP BY
            b.payout_date,
            business,
            LOB,
            b.incentive_type,
            b.dealer_code,
            substring(cast(b.activation_month AS text), 5, 2),
            substring(cast(b.activation_month AS text), 1, 4),
            cast(date_part('year', b.payout_date) AS text),
            cast(date_part('month', b.payout_date) AS text),
            CASE
                WHEN post2pre IS NOT NULL THEN 'Y'
                ELSE 'N'
            END,
            CASE
                WHEN mnp IS NOT NULL THEN 'Y'
                ELSE 'N'
            END,
            rate_plan,
            evaluation_month,
            appr_userid
        HAVING
            SUM(b.amount) <> 0
    ) d
    LEFT JOIN sdbsrc.src_dealer_profile C ON d.dealer_code = C .dealer_code
WHERE
    (payout_date - INTERVAL '1 day') = C .data_date;