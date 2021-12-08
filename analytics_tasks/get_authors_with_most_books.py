from analytics_tasks_types import CountingByFieldTask


class CountingByAuthorTask(CountingByFieldTask):
    def __init__(self, label='', top_count=10, name='', description='', file_name=''):
        super().__init__(name, description, file_name, top_count, label)

    def _get_specific_data(self, df):
        df = df.select('author')
        df = df.filter(df.author.isNotNull())

        return df
