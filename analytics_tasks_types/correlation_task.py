import plotly.graph_objects as go

from .base_analytics import BaseAnalytics
from pyspark.mllib.stat import Statistics
from plotly_scatter_confidence_ellipse import confidence_ellipse
from scipy.stats import linregress, rankdata


class CorrelationTask(BaseAnalytics):
    __abstract__ = True

    def __init__(self, name, description, xaxis_title, yaxis_title, html_file_name, is_need_visualise=True,
                 is_ranking_corr_task=True):
        super().__init__(name, description, is_need_visualise)
        self.correlation = None
        self.records = None
        self.xaxis_title = xaxis_title
        self.yaxis_title = yaxis_title
        self.html_file_name = html_file_name
        self.is_ranking_corr_task = is_ranking_corr_task

    def _prepare_output_data(self):
        x = self.df.rdd.map(lambda r: r[0])
        y = self.df.rdd.map(lambda r: r[1])
        self.correlation = Statistics.corr(x, y, method='spearman')
        self.records = self.df.collect()

    def _visualize(self):
        x, y = tuple(zip(*self.records))
        if self.is_ranking_corr_task:
            x_data = rankdata(x, method='ordinal')
            y_data = rankdata(y, method='ordinal')
        else:
            x_data = x
            y_data = y

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=x_data, y=y_data, mode='markers', name='Книги'))

        slope, intercept, rvalue, pvalue, stderr = linregress(x_data, y_data)
        fig.add_trace(go.Scatter(x=x_data, y=[slope * x + intercept for x in x_data],
                                 name='Линейная регрессия'))

        ellipse_coords_x, ellipse_coords_y = confidence_ellipse(x_data, y_data)
        fig.add_trace(go.Scatter(x=ellipse_coords_x, y=ellipse_coords_y, line={'color': 'black', 'dash': 'dot'},
                                 name='Эллипс 95%-ой доверительной области'))

        corr = float(format(self.correlation, '.2f'))
        fig.update_layout(width=1000, height=1000, title=f'Коэффициент корреляции Спирмена = {corr}',
                          xaxis_title=self.xaxis_title, yaxis_title=self.yaxis_title)
        fig.update_yaxes(scaleanchor='x', scaleratio=1)
        fig.write_html(f'./visualizations/{self.html_file_name}')
        return f'visualizations/{self.html_file_name}'
