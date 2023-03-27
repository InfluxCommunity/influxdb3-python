#!/usr/bin/env python3

import cmd
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

    def do_query(self, arg):
        'Run a SQL query: query [sql]'
        self._execute()(arg)

    def _execute(self, arg):
        try: 
            query = self._flight_sql_client.execute(arg)
            reader = self._flight_sql_client.do_get(query.endpoints[0].ticket)
            table = reader.read_all()
            print(table.to_pandas().to_markdown())
        except Exception as e:
            print(e)

    def do_exit(self, arg):
        'Exit the shell: exit'
        print('Exiting the CLIApp shell...')
        return True

    def do_EOF(self, arg):
        'Exit the shell with Ctrl-D'
        return self.do_exit(arg)

    def precmd(self, line):
        if line.strip() == 'query':
            while True:
                try:
                    statement = self.prompt_session.prompt('(query >) ')
                    if statement.strip().lower() == 'exit':
                        break
                    self._execute(statement)
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

if __name__ == '__main__':
    app = IOXCLI()
    app.cmdloop()