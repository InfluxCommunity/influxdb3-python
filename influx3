#!/usr/bin/env python3

import cmd
import argparse
from flightsql import FlightSQLClient
import json
from prompt_toolkit import PromptSession
from prompt_toolkit.lexers import PygmentsLexer
from pygments.lexers import SqlLexer
from influxdb_client_3 import InfluxDBClient3
import os

_usage_string = """
to write data use influxdb line protocol:
> influx3 write testmes,tag1=tagvalue field1=0.0 <optional timestamp>

to read data with sql:
> influx3 sql select * from testmes where time > now() - interval'1 minute'

to enter interactive mode:
> influx3
"""

_description_string = 'CLI application for Querying IOx with arguments and interactive mode.'

class IOXCLI(cmd.Cmd):
    intro = 'Welcome to my IOx CLI.\n'
    prompt = '(>) '

    def __init__(self):
        super().__init__()
        self._configurations = {}
        self._load_config()
        self._sql_prompt_session = PromptSession(lexer=PygmentsLexer(SqlLexer))
        self._write_prompt_session = PromptSession(lexer=None)

    def do_sql(self, arg):
        if self._configurations == {}:
            print("can't query, no active configs")
            return
        try: 
            reader = self.influxdb_client.query(sql_query=arg)
            table = reader.read_all()
            print(table.to_pandas().to_markdown())
        except Exception as e:
            print(e)

    def do_write(self, arg):
        if self._configurations == {}:
            print("can't write, no active configs")
            return
        if arg == "":
            print("can't write, no line protocol supplied")
            return
        
        self.influxdb_client.write(record=arg)

    def do_exit(self, arg):
        'Exit the shell: exit'
        print('\nExiting ...')
        return True

    def do_EOF(self, arg):
        'Exit the shell with Ctrl-D'
        return self.do_exit(arg)

 
    def precmd(self, line):
        if line.strip() == 'sql':
            self._run_prompt_loop('(sql >) ', self.do_sql, 'SQL mode')
            return ''
        if line.strip() == 'write':
            self._run_prompt_loop('(write >) ', self.do_write, 'write mode')
            return ''
        return line

    def _run_prompt_loop(self, prompt, action, mode_name):
        prompt_session = self._sql_prompt_session if mode_name == 'SQL mode' else self._write_prompt_session
        while True:
            try:
                statement = prompt_session.prompt(prompt)
                if statement.strip().lower() == 'exit':
                    break
                action(statement)
            except KeyboardInterrupt:
                print(f'Ctrl-D pressed, exiting {mode_name}...')
                break
            except EOFError:
                print(f'Ctrl-D pressed, exiting {mode_name}...')
                break
    
    def config(self, args):
        if args.name in self._configurations:
            config = self._configurations[args.name]
        else:
            config = {}

        attributes = ['namespace', 'host', 'token', 'org']

        for attribute in attributes:
            arg_value = getattr(args, attribute)
            if arg_value is not None:
                config[attribute] = arg_value

        config['active'] = True

        missing_attributes = [attribute for attribute in attributes if attribute not in config]

        if missing_attributes:
            print(f"configuration {args.name} is missing the following required attributes: {missing_attributes}")

        self._configurations[args.name] = config
        with open('config.json', 'w') as f:
            f.write(json.dumps(self._configurations))
        
    
    def _load_config(self):
        if not os.path.exists('config.json'):
            return
        f = open('config.json', 'r')

        self._configurations = json.loads(f.read())
        active_conf = None
        for c in self._configurations.keys():
            if self._configurations[c]["active"]:
                active_conf = self._configurations[c]
        if active_conf is None:
            print("no active configuration found")
        self._namespace = active_conf['namespace']

        self.influxdb_client = InfluxDBClient3(host=f"{active_conf['host']}",
                                                 org=active_conf['org'],
                                                 token=active_conf['token'],
                                                 namespace=active_conf['namespace']
                                                 )

class StoreRemainingInput(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, ' '.join(values))

def parse_args():
    parser = argparse.ArgumentParser(description= _description_string
                                     )
    subparsers = parser.add_subparsers(dest='command')

    sql_parser = subparsers.add_parser('sql', help='execute the given SQL query')
    sql_parser.add_argument('query', metavar='QUERY', nargs='*', action=StoreRemainingInput, help='the SQL query to execute')

    write_parser = subparsers.add_parser('write', help='write line protocol to InfluxDB')
    write_parser.add_argument('line_protocol', metavar='LINE PROTOCOL',  nargs='*', action=StoreRemainingInput, help='the data to write')

    config_parser = subparsers.add_parser("config", help="configure the application")
    config_parser.add_argument("--name", help="Configuration name", required=True)
    config_parser.add_argument("--host", help="Host string")
    config_parser.add_argument("--token", help="Token string")
    config_parser.add_argument("--namespace", help="Namespace string")
    config_parser.add_argument("--org", help="Organization string")

    config_parser = subparsers.add_parser("help")

    return parser.parse_args()

def main():
    args = parse_args()
    app = IOXCLI()

    if args.command == 'sql':
        app.do_sql(args.query)
    if args.command == 'write':
        app.do_write(args.line_protocol)
    if args.command == 'config':
        app.config(args)
    if args.command == 'help':
        print(_usage_string)
    if args.command is None:
        app.cmdloop()
    

if __name__ == '__main__':
    main()

