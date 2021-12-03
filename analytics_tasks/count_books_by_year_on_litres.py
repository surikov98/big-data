from analytics_tasks_types import CountingByFieldTask


class CountingByLitresDateTask(CountingByFieldTask):
    def __init__(self, xaxis_title=None, yaxis_title=None, html_file_name=None, is_need_visualise=False, top_count=10):
        super().__init__(top_count, xaxis_title, yaxis_title, html_file_name, is_need_visualise)

    def _get_data_frame(self):
        df = self.df.select('date_litres')
        df = df.filter(df.date_litres.isNotNull())

        self.df = df

    def run_process(self):
        print('Counting books by litres date...')
        super(CountingByLitresDateTask, self).run_process()
