import json

from time import time


class BaseAnalytics:
    __abstract__ = True

    task_list = []

    def __init__(self, name, description, file_name):
        self.df = None
        self.name = name
        self.description = description
        self.file_name = file_name
        self._get_specific_data_time = None
        self._prepare_output_data_time = None
        self._visualize_time = None

    def _prepare_output_data(self):
        pass

    def _get_specific_data(self, df):
        pass

    def _visualize(self):
        pass

    def run_process(self, df):
        print(f"Run process '{self.name}'")
        print(f"About process: '{self.description}'")
        start_time = time()
        self.df = self._get_specific_data(df)
        self._get_specific_data_time = (time() - start_time) * 1000
        start_time = time()
        self._prepare_output_data()
        self._prepare_output_data_time = (time() - start_time) * 1000
        start_time = time()
        filenames = self._visualize()
        self._visualize_time = (time() - start_time) * 1000
        with open(f'./analytics_results/time_measurements/{self.file_name}_ms_time.json', 'w') as f:
            json.dump(self.get_request_time(), f)
            filenames.append(f'analytics_results/time_measurements/{self.file_name}_ms_time.json')
        return filenames

    @classmethod
    def register_task(cls, task):
        BaseAnalytics.task_list.append(task)

    def get_request_time(self):
        total_time = self._prepare_output_data_time + self._get_specific_data_time + self._visualize_time
        return {"get_specific_data_time": self._get_specific_data_time,
                "prepare_output_data_time": self._prepare_output_data_time, "visualize_time": self._visualize_time,
                "total_time": total_time}
