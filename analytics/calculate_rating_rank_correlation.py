import argparse
import plotly.graph_objects as go

from pyspark.mllib.stat import Statistics
from scipy.stats import linregress, rankdata

from plotly_scatter_confidence_ellipse import confidence_ellipse
from utils import get_data_frame_from_mongodb, update_argument_parser_mongodb


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='')
    update_argument_parser_mongodb(parser)

    return parser.parse_args()


def calculate_rating_rank_correlation(args):
    df = get_data_frame_from_mongodb(args.database, args.username, args.password, args.host, args.port,
                                     args.authenticationDatabase)
    df = df.select('average_rating_litres', 'average_rating_livelib')
    df = df.filter((df.average_rating_litres != 0) & (df.average_rating_livelib != 0) &
                   df.average_rating_litres.isNotNull() & df.average_rating_livelib.isNotNull())

    rating_litres = df.rdd.map(lambda r: r[0])
    rating_livelib = df.rdd.map(lambda r: r[1])

    corr = Statistics.corr(rating_litres, rating_livelib, method='spearman')
    records = df.collect()
    return corr, records


def visualize(corr, records):
    rating_litres, rating_livelib = tuple(zip(*records))
    rating_litres_ranks = rankdata(rating_litres, method='ordinal')
    rating_livelib_ranks = rankdata(rating_livelib, method='ordinal')

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=rating_litres_ranks, y=rating_livelib_ranks, mode='markers', name='Книги'))

    slope, intercept, rvalue, pvalue, stderr = linregress(rating_litres_ranks, rating_livelib_ranks)
    fig.add_trace(go.Scatter(x=rating_litres_ranks, y=[slope * x + intercept for x in rating_litres_ranks],
                             name='Линейная регрессия'))

    ellipse_coords_x, ellipse_coords_y = confidence_ellipse(rating_litres_ranks, rating_livelib_ranks)
    fig.add_trace(go.Scatter(x=ellipse_coords_x, y=ellipse_coords_y, line={'color': 'black', 'dash': 'dot'},
                             name='Эллипс 95%-ой доверительной области'))

    corr = float(format(corr, '.2f'))
    fig.update_layout(width=1000, height=1000, title=f'Коэффициент корреляции Спирмена = {corr}',
                      xaxis_title='Ранг рейтинга литреса', yaxis_title='Ранг рейтинга лайвлиба')
    fig.update_yaxes(scaleanchor='x', scaleratio=1)
    fig.write_html('visualizations/rating_rank_correlation.html')


if __name__ == '__main__':
    args = register_launch_arguments()
    corr, records = calculate_rating_rank_correlation(args)
    visualize(corr, records)
