select postal_code, temperature_air_2m_f
    from onpoint_id.history_hour
    where country='US' and LEFT(postal_code, 2) = '94' and time_valid_utc = %s