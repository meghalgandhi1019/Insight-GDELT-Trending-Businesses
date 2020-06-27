
/* Grafana queries for drop down menu #
SELECT 'Health' AS __text, 'HLH' AS __value UNION ALL
    SELECT 'Agriculture' AS __text, 'AGR' AS __value UNION ALL
    SELECT 'Environment Sector' AS __text, 'ENV' AS __value UNION ALL
    SELECT 'Government' AS __text, 'GOV' AS __value UNION ALL
    SELECT 'IT-Business' AS __text, 'BUS' AS __value UNION ALL
    SELECT 'Media Sector' AS __text, 'MED' AS __value UNION ALL
    SELECT 'Education' AS __text, 'EDU' AS __value UNION ALL
    SELECT 'Non-profits NGO' AS __text, 'NGO' AS __value

SELECT 'Alabama' AS __text, 'AL' AS __value UNION ALL
    SELECT 'Alaska' AS __text, 'AK' AS __value UNION ALL
    SELECT 'Arizona' AS __text, 'AZ' AS __value UNION ALL
    SELECT 'Arkansas' AS __text, 'AR' AS __value UNION ALL
    SELECT 'California' AS__text, 'CA' AS__value UNION ALL
    SELECT 'Colorado' AS__text, 'CO' AS__value UNION ALL
    SELECT 'Connecticut' AS__text, 'CT' AS__value UNION ALL
    SELECT 'Delaware' AS__text, 'DE' AS__value UNION ALL
    SELECT 'District of Columbia' AS__text, 'DC' AS__value UNION ALL
    SELECT 'Florida' AS__text, 'FL' AS__value UNION ALL
    SELECT 'Georgia' AS__text, 'GA' AS__value UNION ALL
    SELECT 'Hawaii' AS__text, 'HI' AS__value UNION ALL
    SELECT 'Idaho' AS__text, 'ID' AS__value UNION ALL
    SELECT 'Illinois' AS__text, 'IL' AS__value UNION ALL
    SELECT 'Indiana' AS__text, 'IN' AS__value UNION ALL
    SELECT 'Iowa' AS__text, 'IA' AS__value UNION ALL
    SELECT 'Kansas' AS__text, 'KS' AS__value UNION ALL
    SELECT 'Kentucky' AS__text, 'KY' AS__value UNION ALL
    SELECT 'Louisiana' AS__text, 'LA' AS__value UNION ALL
    SELECT 'Maine' AS__text, 'ME' AS__value UNION ALL
    SELECT 'Maryland' AS__text, 'MD' AS__value UNION ALL
    SELECT 'Massachusetts' AS__text, 'MA' AS__value UNION ALL
    SELECT 'Michigan' AS__text, 'MI' AS__value UNION ALL
    SELECT 'Minnesota' AS__text, 'MN' AS__value UNION ALL
    SELECT 'Mississippi' AS__text, 'MS' AS__value UNION ALL
    SELECT 'Missouri' AS__text, 'MO' AS__value UNION ALL
    SELECT 'Montana' AS__text, 'MT' AS__value UNION ALL
    SELECT 'Nebraska' AS__text, 'NE' AS__value UNION ALL
    SELECT 'Nevada' AS__text, 'NV' AS__value UNION ALL
    SELECT 'New Hampshire' AS__text, 'NH' AS__value UNION ALL
    SELECT 'New Jersey' AS__text, 'NJ' AS__value UNION ALL
    SELECT 'New Mexico' AS__text, 'NM' AS__value UNION ALL
    SELECT 'New York' AS__text, 'NY' AS__value UNION ALL
    SELECT 'North Carolina' AS__text, 'NC' AS__value UNION ALL
    SELECT 'North Dakota' AS__text, 'ND' AS__value UNION ALL
    SELECT 'Ohio' AS__text, 'OH' AS__value UNION ALL
    SELECT 'Oklahoma' AS__text, 'OK' AS__value UNION ALL
    SELECT 'Oregon' AS__text, 'OR' AS__value UNION ALL
    SELECT 'Pennsylvania' AS__text, 'PA' AS__value UNION ALL
    SELECT 'Rhode Island' AS__text, 'RI' AS__value UNION ALL
    SELECT 'South Carolina' AS__text, 'SC' AS__value UNION ALL
    SELECT 'South Dakota' AS__text, 'SD' AS__value UNION ALL
    SELECT 'Tennessee' AS__text, 'TN' AS__value UNION ALL
    SELECT 'Texas' AS__text, 'TX' AS__value UNION ALL
    SELECT 'Utah' AS__text, 'UT' AS__value UNION ALL
    SELECT 'Vermont' AS__text, 'VT' AS__value UNION ALL
    SELECT 'Virginia' AS__text, 'VA' AS__value UNION ALL
    SELECT 'Washington' AS__text, 'WA' AS__value UNION ALL
    SELECT 'West Virginia' AS__text, 'WV' AS__value UNION ALL
    SELECT 'Wisconsin' AS__text, 'WI' AS__value UNION ALL
    SELECT 'Wyoming' AS__text, 'WY' AS__value

/* Grafana dashboard queries */
SELECT monthyear as time,
(goldsteinscale_norm_sum*100)/(select  sum(goldsteinscale_norm_sum) from gdeltdetails where state = '$state' AND $__timeFilter(monthyear) group by state, monthyear ) impact ,
(confidence_sum*100)/(select  sum(confidence_sum) from gdeltdetails where state = '$state' AND $__timeFilter(monthyear) group by state, monthyear ) confidence, state as "metric" from gdeltdetails
where state = '${state}'
  AND actor1type1code = '${Industries}'
  AND $__timeFilter(monthyear)
order by 1;


SELECT monthyear as time, (goldsteinscale_norm_sum/event_counts) impact ,  (confidence_sum/event_counts) confidence, state as "metric" from gdeltdetails
where state = '${state}' AND actor1type1code = '${Industries}' AND $__timeFilter(monthyear) order by 1;

