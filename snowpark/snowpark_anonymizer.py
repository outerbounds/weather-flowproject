from metaflow import (
    FlowSpec,
    step,
    card,
    Parameter,
    trigger_on_finish,
    project,
    current,
    pypi,
    Config,
    config_expr,
)
from metaflow import snowpark
from metaflow.cards import Table, Markdown

from flowproject import BaseFlow


@project(name=config_expr("flowconfig.project_name"))
@trigger_on_finish(flow="SensorFlow")
class SnowparkAnonymizerFlow(BaseFlow):

    snowpark_config = Config("snowpark_config", default="snowpark.json")
    snowpark_session = Config("snowpark_session", default="snowpark_session.json")
    tablename = Parameter("tablename", default="villetesttable")

    @pypi(
        python="3.11",
        packages={
            "snowflake": "1.0.2",
            "pandas": "2.2.3",
            "snowflake-snowpark-python": "1.26.0",
        },
    )
    @snowpark(**snowpark_config)
    @card
    @step
    def start(self):
        from snowflake.snowpark import Session
        print('Executing inside Snowflake')
        sess = Session.builder.configs(self.snowpark_session).create()
        df = sess.table(self.tablename)
        self.rows = []
        self.cols = df.columns
        for row in df.to_local_iterator():
            d = row.as_dict()
            self.rows.append([d[c][0] if c == "LASTNAME" else d[c] for c in self.cols])
        current.card.append(Markdown(f"# Anonymized Data `{self.tablename}`"))
        current.card.append(Table(self.rows, headers=self.cols))
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    SnowparkAnonymizerFlow()
