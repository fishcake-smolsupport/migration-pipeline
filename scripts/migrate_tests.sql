SELECT TOP 5 dealer_code, payout_month, payout_year, SUM(CAST(incentive AS FLOAT)) 
FROM simulator_based_on_incentive_postpaid_temp 
GROUP BY dealer_code, payout_month, payout_year
ORDER BY NEWID();

SELECT TOP 5 dealer_code, payout_month, payout_year, SUM(CAST(incentive AS FLOAT)) 
FROM simulator_based_on_incentive_prepaid_temp 
GROUP BY dealer_code, payout_month, payout_year
ORDER BY NEWID();