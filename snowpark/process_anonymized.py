from metaflow import step, trigger_on_finish, current, project, Config, config_expr, card
from metaflow.cards import Markdown

from flowproject import BaseFlow, snowflake

@project(name=config_expr("flowconfig.project_name"))
@trigger_on_finish(flow='SnowparkAnonymizerFlow')
class ProcessAnonymizedFlow(BaseFlow):

    @step
    def start(self):
        self.anonymized_rows = current.trigger.run.data.rows
        print(f'Extracted {len(self.anonymized_rows)} from Snowflake')
        self.next(self.end)
    
    @step
    def end(self):
        pass

if __name__ == '__main__':
    ProcessAnonymizedFlow()

