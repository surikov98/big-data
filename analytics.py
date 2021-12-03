from analytics_tasks import RatingsCorrelationTask, DatesCorrelationTask, CountingByLitresDateTask
from analytics_tasks_types import BaseAnalytics


if __name__ == '__main__':
    rct = RatingsCorrelationTask(xaxis_title='Ранг рейтинга литреса', yaxis_title='Ранг рейтинга лайвлиба',
                                 html_file_name='rating_rank_correlation.html')
    BaseAnalytics.register_task(rct)
    dct = DatesCorrelationTask(xaxis_title='Ранг года выхода на Литресс', yaxis_title='Ранг года написания',
                               html_file_name='correlation_data_litres_date_writing.html')
    BaseAnalytics.register_task(dct)
    cbldt = CountingByLitresDateTask(top_count=5)
    BaseAnalytics.register_task(cbldt)
    BaseAnalytics.run_all()