#!/usr/bin/env python3

import cmd
import argparse
from flightsql import FlightSQLClient
import json
from prompt_toolkit import PromptSession
from prompt_toolkit.lexers import PygmentsLexer
from pygments.lexers import SqlLexer

class IOXCLI(cmd.Cmd):
    intro = 'Welcome to my IOx CLI.\n'
    prompt = '(>) '

    def __init__(self):
        super().__init__()
        self.prompt_session = PromptSession(lexer=PygmentsLexer(SqlLexer))

        self._load_config()   

    def do_sql(self, arg):
        'Run a SQL query: sql [query]'
        self.execute()(arg)

    def execute(self, arg):
        try: 
            query = self._flight_sql_client.execute(arg)
            reader = self._flight_sql_client.do_get(query.endpoints[0].ticket)
            table = reader.read_all()
            print(table.to_pandas().to_markdown())
        except Exception as e:
            print(e)

    def do_exit(self, arg):
        'Exit the shell: exit'
        print('Exiting ...')
        return True

    def do_EOF(self, arg):
        'Exit the shell with Ctrl-D'
        return self.do_exit(arg)

    def precmd(self, line):
        if line.strip() == 'sql':
            while True:
                try:
                    statement = self.prompt_session.prompt('(sql >) ')
                    if statement.strip().lower() == 'exit':
                        break
                    self.execute(statement)
                except KeyboardInterrupt:
                    print('Ctrl-D pressed, exiting SQL mode...')
                    break
            return ''
        return line
    
    def _load_config(self):
        f = open('config.json')
        conf = json.loads(f.read())
        self._flight_sql_client = FlightSQLClient(host=conf['host'],
                                                  token=conf['token'],
                                                  metadata={'bucket-name':conf['bucket']})

class StoreRemainingInput(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, ' '.join(values))

def parse_args():
    parser = argparse.ArgumentParser(description='CLI application for Querying IOx with arguments and interactive mode.')
    subparsers = parser.add_subparsers(dest='command')

    sql_parser = subparsers.add_parser('sql', help='execute the given SQL query')
    sql_parser.add_argument('query', metavar='QUERY', nargs='*', action=StoreRemainingInput, help='the SQL query to execute')

    return parser.parse_args()

def main():
    args = parse_args()
    app = IOXCLI()

    if args.command == 'sql':
        print(args.query)
        app.execute(args.query)
    if args.command is None:
        app.cmdloop()


if __name__ == '__main__':
    main()

