from datetime import datetime, timedelta

from metaflow import (
    step,
    trigger_on_finish,
    current,
    project,
    Config,
    config_expr,
    card,
    Parameter,
    pypi,
    resources,
)
from metaflow.cards import Markdown, VegaChart, ProgressBar

from flowproject import BaseFlow, snowflake


@project(name=config_expr("flowconfig.project_name"))
@trigger_on_finish(flow="SensorFlow")
class ForecastFlow(BaseFlow):

    window = Parameter("window", default=18, help="past time window for forecast")
    qa_zipcode = Parameter(
        "qa_zipcode", default="02108", help="visualize this zipcode for sanity check"
    )

    @snowflake
    @step
    def start(self):
        self.timestamp = self.sensor_value
        self.since = datetime.fromisoformat(self.timestamp[:-1]) - timedelta(
            days=self.window
        )
        print("Querying data since", self.since)
        self.data = self.query_snowflake(
            template=("historical_temperature", [self.since])
        )
        self.next(self.forecast)

    @card(type="blank", refresh_interval=1)
    @pypi(
        python="3.11",
        packages={"pandas": "2.2.3", "sktime": "0.35.0", "statsmodels": "0.14.4"},
    )
    @resources(cpu=2, memory=8000)
    @step
    def forecast(self):
        from weather import forecast

        self.forecasts = {}
        zipcode_series = forecast.make_series(self.data)
        p = ProgressBar(max=len(zipcode_series), label="Forecasts done")
        current.card.append(p)
        for i, (zipcode, series) in enumerate(zipcode_series):
            try:
                past = forecast.series_to_list(series)
                future = forecast.forecast(series)
            except:
                pass
            else:
                self.forecasts[zipcode] = (past, future)
            p.update(i + 1)
            current.card.refresh()
        self.next(self.end)

    @card(type="blank")
    @step
    def end(self):
        if self.qa_zipcode in self.forecasts:
            from weather import forecastviz

            past, future = self.forecasts[self.qa_zipcode]
            current.card.append(Markdown(f"# Forecast for {self.qa_zipcode}"))
            current.card.append(
                Markdown("**Blue** is historical data, **orange** is our forecast")
            )
            current.card.append(VegaChart(forecastviz.vegaspec(past, future)))
        else:
            current.card.append(Markdown(f"# {self.qa_zipcode} not found"))


if __name__ == "__main__":
    ForecastFlow()
