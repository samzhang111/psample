#============================================================================
# psample.py
#
# Sample n random rows from a PostgreSQL database and outputs in properly
# encoded csv format (to file or to stdout).
#
# The problem with "psql ... WITH DELIMITER ',' CSV HEADER > ..." is that it 
# does not properly encode CSV files.
#
##===========================================================================


import psycopg2

def get_database_args():
    import argparse
    import getpass
    import sys

    parser = argparse.ArgumentParser(description='Randomly sample rows from a '
            'Postgres database', add_help=False)
    parser.add_argument('-h', '--host', required=True, help='Hostname')
    parser.add_argument('-u', '--username', required=True, help='Username')
    parser.add_argument('-d', '--database', required=True, help='Database name')
    parser.add_argument('-t', '--table', required=True, help='Table name')
    parser.add_argument('-n', '--numrows', required=True, 
            type=int, help='Number of rows to sample')
    parser.add_argument('-o', '--outfile', default=sys.stdout, help='CSV File '
            'to output to. Default: stdout')
    args = parser.parse_args()
    
    password = getpass.getpass('\nPassword: ')

    return args, password

class DatabaseSampler(object):
    """
    DatabaseSampler
    """

    def __init__(self, host='localhost', port=5432, 
            username='', password='', db_name='db', db_dict={}):
        """
        A DatabaseSampler object connects to a PostgreSQL database using
        psycopg2.
        """
        if db_dict:
            self.DATABASE = db_dict
        else:
            self.DATABASE = {
                    'host': host,
                    'port': port,
                    'user': username,
                    'password': password,
                    'database': db_name 
                    }
        self.conn = psycopg2.connect(**self.DATABASE)

    def sample(self, table, n, out):
        """
        Sample n rows from table, optionally output as csv into filename 'out'.
        """
        from psycopg2.extras import DictCursor
        import csv
        cur = self.conn.cursor(cursor_factory=DictCursor)

        # since it's unsafe to interpolate values into a SQL query,
        # but since we have no other choice with table names, we should first
        # verify that it is a valid table in the database:
        cur.execute('SELECT column_name FROM information_schema.columns '
        'WHERE table_name=%s',(table,))

        columns = cur.fetchall()
        
        if columns:
            columns = [[x[0] for x in columns]]

            # slightly less bad because of the verification above:
            cur.execute('SELECT * FROM {table} ORDER BY random() LIMIT %s'
                    .format(table=table),
                    (n,))
            
            results = cur.fetchall()
        else:
            raise psycopg2.ProgrammingError('Table {table} does not exist'
                    .format(table=table))
        
        columns.extend(results)
        if out != sys.stdout:
            out = open(out, 'w')
            
        writer = csv.writer(out, quoting=csv.QUOTE_MINIMAL)
        writer.writerows(columns)

        return columns

if __name__ == '__main__':
    
    args, password = get_database_args()
    ds = DatabaseSampler(host=args.host,
            username=args.username,
            db_name=args.database,
            password=password)
    
    ds.sample(args.table, args.numrows, out=args.outfile)
    

