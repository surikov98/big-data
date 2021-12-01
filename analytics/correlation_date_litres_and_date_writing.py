import argparse
import plotly.graph_objects as go

from pyspark.mllib.stat import Statistics
from scipy.stats import linregress, rankdata

from plotly_scatter_confidence_ellipse import confidence_ellipse
from utils import get_data_frame_from_mongodb, update_argument_parser_mongodb


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Correlation of the number of reviews and the reviews of the book')
    update_argument_parser_mongodb(parser)

    return parser.parse_args()


def calculate_correlation_reviews_rating(args):
    df = get_data_frame_from_mongodb(args.database, args.username, args.password, args.host, args.port,
                                     args.authenticationDatabase)
    df = df.select('date_litres', 'date_writing')
    df = df.filter(df.date_litres.isNotNull() & df.date_writing.isNotNull())

    date_litres = df.rdd.map(lambda r: r[0])
    date_writing = df.rdd.map(lambda r: r[1])

    corr = Statistics.corr(date_litres, date_writing, method='spearman')
    records = df.collect()
    return corr, records


def visualize(corr, records):
    date_litres, date_writing = tuple(zip(*records))
    date_litres_ranks = rankdata(date_litres, method='ordinal')
    date_writing_ranks = rankdata(date_writing, method='ordinal')

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=date_litres_ranks, y=date_writing_ranks, mode='markers', name='Книги'))

    slope, intercept, rvalue, pvalue, stderr = linregress(date_litres_ranks, date_writing_ranks)
    fig.add_trace(go.Scatter(x=date_litres_ranks, y=[slope * x + intercept for x in date_litres_ranks],
                             name='Линейная регрессия'))

    ellipse_coords_x, ellipse_coords_y = confidence_ellipse(date_litres_ranks, date_writing_ranks)
    fig.add_trace(go.Scatter(x=ellipse_coords_x, y=ellipse_coords_y, line={'color': 'black', 'dash': 'dot'},
                             name='Эллипс 95%-ой доверительной области'))

    corr = float(format(corr, '.2f'))
    fig.update_layout(width=1000, height=1000, title=f'Коэффициент корреляции Спирмена = {corr}',
                      xaxis_title='Ранг года выхода на Литресс', yaxis_title='Ранг года написания')
    fig.update_yaxes(scaleanchor='x', scaleratio=1)
    fig.write_html('visualizations/correlation_data_litres_date_writing.html')


if __name__ == '__main__':
    args = register_launch_arguments()
    corr, records = calculate_correlation_reviews_rating(args)
    visualize(corr, records)
