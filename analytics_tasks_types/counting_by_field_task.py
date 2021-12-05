from operator import add

from .base_analytics import BaseAnalytics


class CountingByFieldTask(BaseAnalytics):
    __abstract__ = True

    def __init__(self, name, description, top_count, xaxis_title=None, yaxis_title=None, html_file_name=None,
                 is_need_visualise=False):
        super().__init__(name, description, is_need_visualise)
        self.data = None
        self.xaxis_title = xaxis_title
        self.yaxis_title = yaxis_title
        self.html_file_name = html_file_name
        self.top_count = top_count

    def _prepare_output_data(self):
        self.data = self.df.rdd.flatMap(lambda data: data).map(lambda data: (data, 1)).reduceByKey(add) \
            .sortBy(lambda x: x[1], ascending=False).collect()

    def _visualize(self):
        count = len(self.data)
        with open("./visualizations/authors_top.txt", 'w') as f:
            if count < self.top_count:
                self.top_count = None
                f.write(f'Top-{count}:\n')
            else:
                f.write(f'Top-{self.top_count}:\n')
            f.writelines(list(f'{idx + 1}. {data}\n' for idx, data in enumerate(self.data[:self.top_count])))
        return f'visualizations/authors_top.txt'

