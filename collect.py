import argparse

from connector import Connector, BOOKS_PER_PAGE
from utils import update_argument_parser_mongodb


def register_launch_arguments():
    parser = argparse.ArgumentParser(description='Serve the connector application')
    update_argument_parser_mongodb(parser)
    parser.add_argument('-s', '--sort', help='sorting', choices=['', 'year', 'popularity', 'title'])
    parser.add_argument('--startPage', help='start book page (from 1)', type=int, default=1)
    parser.add_argument('--endPage', help='end book page', type=int)
    parser.add_argument('--startBook', help='start book index', type=int, choices=list(range(1, BOOKS_PER_PAGE + 1)),
                        default=1)
    parser.add_argument('--endBook', help='end book index', type=int, choices=list(range(1, BOOKS_PER_PAGE + 1)),
                        default=BOOKS_PER_PAGE)
    parser.add_argument('--clearDatabase', help='clear database', action='store_true')
    parser.add_argument('-l', '--logFile', help='path to log file', default='connect.log')

    return parser.parse_args()


if __name__ == '__main__':
    args = register_launch_arguments()
    connector = Connector(args.database, args.username, args.password, args.host, args.port,
                          args.authenticationDatabase, args.sort)
    connector.collect(args.startPage, args.endPage, args.startBook, args.endBook, args.clearDatabase, args.logFile)
