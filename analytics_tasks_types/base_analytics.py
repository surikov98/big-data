import argparse

from utils import get_data_frame_from_mongodb, update_argument_parser_mongodb


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Analytics tasks execute')
    update_argument_parser_mongodb(parser)

    return parser.parse_args()


class BaseAnalytics:
    __abstract__ = True

    task_list = []

    def __init__(self, name, description, is_need_visualise=False):
        self.is_need_visualise = is_need_visualise
        self.df = None
        self.name = name
        self.description = description

    def _prepare_output_data(self):
        pass

    def _get_specific_data(self, df):
        pass

    def _visualize(self):
        pass

    def run_process(self):
        print(f"Run process '{self.name}'")
        print(f"About process: '{self.description}'")
        args = register_launch_arguments()
        df = get_data_frame_from_mongodb(args.database, args.username, args.password, args.host, args.port,
                                         args.authenticationDatabase)
        self.df = self._get_specific_data(df)
        self._prepare_output_data()
        if self.is_need_visualise:
            self._visualize()

    @classmethod
    def register_task(cls, task):
        BaseAnalytics.task_list.append(task)

    @classmethod
    def run_all(cls):
        for task in BaseAnalytics.task_list:
            task.run_process()
