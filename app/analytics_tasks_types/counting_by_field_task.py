import plotly.graph_objects as go

from operator import add

from .base_analytics import BaseAnalytics


class CountingByFieldTask(BaseAnalytics):
    __abstract__ = True

    def __init__(self, name, description, file_name, top_count, label, diagram_mode=True, xaxis_title=None,
                 yaxis_title=None):
        super().__init__(name, description, file_name)
        self.data = None
        self.label = label
        self.top_count = top_count
        self.diagram_mode = diagram_mode
        self.xaxis_title = xaxis_title
        self.yaxis_title = yaxis_title

    def _prepare_output_data(self):
        self.data = self.df.rdd.flatMap(lambda data: data).map(lambda data: (data, 1)).reduceByKey(add) \
            .sortBy(lambda x: x[1], ascending=False).collect()

    def _visualize(self):
        count = len(self.data)
        with open(f'./analytics_results/text_data/{self.file_name}.txt', 'w') as f:
            if count < self.top_count:
                self.top_count = None
                title = f'Top-{count}'
            else:
                title = f'Top-{self.top_count}'
            f.write(f'{title}:\n')
            top_data = self.data[:self.top_count]
            f.writelines(list(f'{idx + 1}. {data}\n' for idx, data in enumerate(top_data)))
            x, y = tuple(zip(*top_data))
            x = [str(x_val) for x_val in x]
            all_count = sum(y)
            if self.diagram_mode:
                labels = [f'{self.label}: {str(all_count)}']
                parents = [""]
                values = [all_count]

                labels += [x_val + f'<br>{y[x.index(x_val)]}' for x_val in x]
                parents += [labels[0]] * len(top_data)
                values += y

                fig = go.Figure(go.Sunburst(
                    labels=labels,
                    parents=parents,
                    values=values,
                    branchvalues="total"
                ))
                fig.update_layout(title=title)
            else:
                fig = go.Figure(data=[go.Bar(x=x, y=y)])
                fig.update_layout(title=f'{self.label}: {str(all_count)}',
                                  xaxis_title=self.xaxis_title,
                                  yaxis_title=self.yaxis_title,
                                  )
            fig.write_html(f'./analytics_results/visualizations/{self.file_name}.html')
        return [f'analytics_results/text_data/{self.file_name}.txt',
                f'analytics_results/visualizations/{self.file_name}.html']

