# psample.py

Randomly sample rows from a Postgres database and export in CSV format.

Usage: python psample.py -h HOST -u USERNAME -d DATABASE -t TABLE -n NUMROWS
[-o OUTFILE]

If no outfile is provided, the output is written to stdout.
