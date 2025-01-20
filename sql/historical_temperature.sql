select postal_code,
       time_valid_utc as timestamp,
       temperature_air_2m_f as temperature
    from onpoint_id.history_hour
    where country='US' and time_valid_utc > %s
    order by time_valid_utc asc 