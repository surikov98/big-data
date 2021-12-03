import abc

from operator import add

from .base_analytics import BaseAnalytics


class CountingByFieldTask(BaseAnalytics):

    def __init__(self, top_count, xaxis_title=None, yaxis_title=None, html_file_name=None, is_need_visualise=False):
        super().__init__(is_need_visualise)
        self.data = None
        self.xaxis_title = xaxis_title
        self.yaxis_title = yaxis_title
        self.html_file_name = html_file_name
        self.top_count = top_count

    def _request_data(self):
        self.data = self.df.rdd.flatMap(lambda data: data).map(lambda data: (data, 1)).reduceByKey(add) \
            .sortBy(lambda x: x[1], ascending=False).collect()

    @abc.abstractmethod
    def _get_data_frame(self):
        pass

    def _visualize(self):
        pass

    def run_process(self):
        super(CountingByFieldTask, self).run_process()
        count = len(self.data)
        if count < self.top_count:
            self.top_count = None
            print(f'Top-{count}:')
        else:
            print(f'Top-{self.top_count}:')
        print(*self.data[:self.top_count], sep='\n')
