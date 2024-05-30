SELECT
    toStartOfDay(pickup_datetime) AS day,
    payment_type,
    count(trip_id) AS rides,
    floor(sum(total_amount), 2) AS amount
FROM default.trips
WHERE toStartOfDay(pickup_datetime) = toDate('{{date}}')
GROUP BY toStartOfDay(pickup_datetime), payment_type;
