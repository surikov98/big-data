from app.analytics_tasks_types import CountingByFieldTask


class CountingByLitresDateTask(CountingByFieldTask):
    def __init__(self, label='', top_count=10, name='', description='', file_name=''):
        super().__init__(name, description, file_name, top_count, label, diagram_mode=False, xaxis_title='Года издания',
                         yaxis_title='Количество книг')

    def _get_specific_data(self, df):
        df = df.select('date_litres')
        df = df.filter(df.date_litres.isNotNull())

        return df
