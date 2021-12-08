from app.analytics_tasks_types import CountingByFieldTask


class CountingByLitresDateTask(CountingByFieldTask):
    def __init__(self, xaxis_title=None, yaxis_title=None, html_file_name=None, is_need_visualise=False, top_count=10,
                 name='', description=''):
        super().__init__(name, description, top_count, xaxis_title, yaxis_title, html_file_name, is_need_visualise)

    def _get_specific_data(self, df):
        df = df.select('date_litres')
        df = df.filter(df.date_litres.isNotNull())

        return df
