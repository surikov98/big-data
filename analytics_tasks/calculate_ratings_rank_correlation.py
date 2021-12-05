from analytics_tasks_types import CorrelationTask


class RatingsCorrelationTask(CorrelationTask):
    def __init__(self, xaxis_title, yaxis_title, html_file_name, is_need_visualise=True, is_ranking_corr_task=True,
                 name='', description=''):
        super().__init__(name, description, xaxis_title, yaxis_title, html_file_name, is_need_visualise,
                         is_ranking_corr_task)

    def _get_specific_data(self, df):
        df = df.select('average_rating_litres', 'average_rating_livelib')
        df = df.filter((df.average_rating_litres != 0) & (df.average_rating_livelib != 0) &
                       df.average_rating_litres.isNotNull() & df.average_rating_livelib.isNotNull())

        return df
