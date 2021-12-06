from analytics_tasks_types import CorrelationTask


class MarksAndCommentsCountsCorrelation(CorrelationTask):
    def __init__(self, xaxis_title, yaxis_title, is_ranking_corr_task=True, name='', description='', file_name=''):
        super().__init__(name, description, file_name, xaxis_title, yaxis_title, is_ranking_corr_task)

    def _get_specific_data(self, df):
        df = df.select('votes_count_litres', 'reviews_count')
        df = df.filter((df.votes_count_litres != 0) & (df.reviews_count != 0) & df.votes_count_litres.isNotNull() &
                       df.reviews_count.isNotNull())

        return df
