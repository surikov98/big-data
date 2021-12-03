import abc
import argparse

from utils import get_data_frame_from_mongodb, update_argument_parser_mongodb


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Analytics tasks execute')
    update_argument_parser_mongodb(parser)

    return parser.parse_args()


class BaseAnalytics(object):
    __metaclass__ = abc.ABCMeta

    task_list = []

    def __init__(self, is_need_visualise=True):
        self.is_need_visualise = is_need_visualise
        self.df = None

    @abc.abstractmethod
    def _request_data(self):
        pass

    @abc.abstractmethod
    def _get_data_frame(self):
        pass

    @abc.abstractmethod
    def _visualize(self):
        pass

    def run_process(self):
        args = register_launch_arguments()
        df = get_data_frame_from_mongodb(args.database, args.username, args.password, args.host, args.port,
                                         args.authenticationDatabase)
        self.df = df
        self._get_data_frame()
        self._request_data()
        if self.is_need_visualise:
            self._visualize()

    @classmethod
    def register_task(cls, task):
        BaseAnalytics.task_list.append(task)

    @classmethod
    def run_all(cls):
        for task in BaseAnalytics.task_list:
            task.run_process()
