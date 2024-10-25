--insert postpaid
INSERT INTO simulator_based_on_incentive
	(payout_date, scheme, lob, incentive_type, dealer_code, act_month, act_year, qty, total_inc, payout_year, payout_month, simulator_type, p2p, mnp, rateplan, payout_period, dealer_name, report_region, new_dealer_class) 
SELECT
	payout_date, business, lob, incentive_type, dealer_code, act_month, act_year, count, incentive, payout_year, payout_month, simulator_type, p2p, mnp, rateplan, incentive_period, name, report_region, dealer_class
FROM simulator_based_on_incentive_postpaid_temp
UNION ALL
SELECT
	payout_date, business, lob, incentive_type, dealer_code, act_month, act_year, count, incentive, payout_year, payout_month, simulator_type, p2p, mnp, rateplan, incentive_period, name, report_region, dealer_class
FROM simulator_based_on_incentive_prepaid_temp;