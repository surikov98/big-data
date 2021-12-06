import argparse
import json

from time import time
from utils import get_data_frame_from_mongodb, update_argument_parser_mongodb


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Analytics tasks execute')
    update_argument_parser_mongodb(parser)

    return parser.parse_args()


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

    @classmethod
    def run_all(cls,):
        args = register_launch_arguments()
        df = get_data_frame_from_mongodb(args.database, args.username, args.password, args.host, args.port,
                                         args.authenticationDatabase)
        for task in BaseAnalytics.task_list:
            task.run_process(df)

    def get_request_time(self):
        total_time = self._prepare_output_data_time + self._get_specific_data_time + self._visualize_time
        return {"get_specific_data_time": self._get_specific_data_time,
                "prepare_output_data_time": self._prepare_output_data_time, "visualize_time": self._visualize_time,
                "total_time": total_time}
