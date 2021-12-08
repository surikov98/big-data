from time import time


class BaseAnalytics:
    __abstract__ = True

    task_list = []

    def __init__(self, name, description, is_need_visualise=False):
        self.is_need_visualise = is_need_visualise
        self.df = None
        self.name = name
        self.description = description
        self._get_specific_data_time = None
        self._prepare_output_data_time = None
        self._visualize_time = None

    def _prepare_output_data(self):
        pass

    def _get_specific_data(self, df):
        pass

    def _visualize(self):
        pass

    def run_process(self, df, return_html=False):
        print(f"Run process '{self.name}'")
        print(f"About process: '{self.description}'")
        start_time = time()
        self.df = self._get_specific_data(df)
        self._get_specific_data_time = (time() - start_time) * 1000
        start_time = time()
        self._prepare_output_data()
        self._prepare_output_data_time = (time() - start_time) * 1000
        if self.is_need_visualise or return_html:
            start_time = time()
            filename = self._visualize()
            self._visualize_time = (time() - start_time) * 1000
            return filename

    @classmethod
    def register_task(cls, task):
        BaseAnalytics.task_list.append(task)

    def get_request_time(self):
        total_time = self._prepare_output_data_time + self._get_specific_data_time
        total_time += self._visualize_time if self._visualize_time is not None else 0
        return {"get_specific_data_time": self._get_specific_data_time,
                "prepare_output_data_time": self._prepare_output_data_time, "visualize_time": self._visualize_time,
                "total_time": total_time}
