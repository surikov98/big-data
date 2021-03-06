from operator import add

from ..analytics_tasks_types import CountingByFieldTask
from ..utils.genres_rev import get_main_genre


class CountingByGenreTask(CountingByFieldTask):
    def __init__(self, label='', top_count=10, name='', description='', file_name=''):
        super().__init__(name, description, file_name, top_count, label)

    def _get_specific_data(self, df):
        df = df.select('genre')
        df = df.filter(df.genre.isNotNull())

        return df

    def _prepare_output_data(self):
        self.data = self.df.rdd.map(lambda data: get_main_genre(data.genre)).filter(lambda data: data != '')\
            .map(lambda data: (data, 1)).reduceByKey(add).sortBy(lambda x: x[1], ascending=False).collect()
