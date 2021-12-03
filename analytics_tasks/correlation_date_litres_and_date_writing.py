from analytics_tasks_types import CorrelationTask


class DatesCorrelationTask(CorrelationTask):
    def __init__(self, xaxis_title, yaxis_title, html_file_name, is_need_visualise=True, is_ranking_corr_task=True):
        super().__init__(xaxis_title, yaxis_title, html_file_name, is_need_visualise, is_ranking_corr_task)

    def _get_data_frame(self):
        df = self.df.select('date_litres', 'date_writing')
        df = df.filter(df.date_litres.isNotNull() & df.date_writing.isNotNull())

        self.df = df
