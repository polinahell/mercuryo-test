SELECT
    date_trunc('day', pickup_datetime) AS day,
    payment_type,
    count(trip_id) AS rides,
    round(sum(total_amount), 2) AS amount
FROM trips
WHERE date_trunc('day', pickup_datetime) = '{{date}}'
GROUP BY day, payment_type;
