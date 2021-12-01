import argparse

from operator import add

from utils import get_data_frame_from_mongodb, update_argument_parser_mongodb


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Number of books in a given publication year')
    update_argument_parser_mongodb(parser)

    return parser.parse_args()


if __name__ == '__main__':
    args = register_launch_arguments()

    df = get_data_frame_from_mongodb(args.database, args.username, args.password, args.host, args.port,
                                     args.authenticationDatabase)
    df = df.select('date_litres')
    df = df.filter(df.date_litres.isNotNull())
    data_book = df.rdd.flatMap(lambda years: years).map(lambda year: (year, 1)).reduceByKey(add)\
        .sortBy(lambda x: x[0], ascending=False).collect()
    print(*data_book, sep='\n')
