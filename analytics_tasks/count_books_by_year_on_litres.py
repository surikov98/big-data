from analytics_tasks_types import CountingByFieldTask


class CountingByLitresDateTask(CountingByFieldTask):
    def __init__(self, xaxis_title=None, yaxis_title=None, top_count=10, name='', description='', file_name=''):
        super().__init__(name, description, file_name, top_count, xaxis_title, yaxis_title)

    def _get_specific_data(self, df):
        df = df.select('date_litres')
        df = df.filter(df.date_litres.isNotNull())

        return df
