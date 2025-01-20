from collections import defaultdict

def make_series(rows):
    import pandas as pd
    values = defaultdict(list)
    tstamps = defaultdict(list)
    for zipcode, hour, temp in rows:
        values[zipcode].append(float(temp))
        tstamps[zipcode].append(hour)
    
    return [(z, pd.Series(data=values[z],
                          index=pd.DatetimeIndex(tstamps[z], freq='infer')))
            for z in values]

def forecast(series, forecast_hours=48):
    import numpy as np
    from sktime.forecasting.theta import ThetaForecaster
    forecaster = ThetaForecaster(sp=forecast_hours)
    forecaster.fit(series)
    pred = forecaster.predict(np.arange(1, forecast_hours))
    return series_to_list(pred)

def series_to_list(series):
    index = map(lambda x: x.isoformat(), series.index)
    return list(zip(index, series))