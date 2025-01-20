from metaflow import step, trigger_on_finish, current, project, Config, config_expr, card
from metaflow.cards import Markdown

from flowproject import BaseFlow, snowflake
from weather.mapviz import make_vegachart

@project(name=config_expr("flowconfig.project_name"))
@trigger_on_finish(flow='SensorFlow')
class WeatherVizFlow(BaseFlow):

    @card(type='blank')
    @snowflake
    @step
    def start(self):
        self.timestamp = self.sensor_value
        print("Querying data for", self.timestamp)
        latest_weather = self.query_snowflake(template=('bay_weather', [self.timestamp]))
        current.card.append(Markdown(f"# Temperatures in the Bay Area at {self.timestamp}"))
        current.card.append(make_vegachart(latest_weather))
        self.next(self.end)
    
    @step
    def end(self):
        pass

if __name__ == '__main__':
    WeatherVizFlow()
