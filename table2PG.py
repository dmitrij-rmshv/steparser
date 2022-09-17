import re
import argparse

import psycopg2

parser = argparse.ArgumentParser(description="Parsing script")
parser.add_argument("-c", dest="create_table", action="store_true", help="create table before filling")
parser.add_argument("input_file", type=str, help="- input file name")
args = parser.parse_args()

h_line = re.compile(r"(^\+-+)+\+")
html_table = True

out = []
cols = []

def fields_description_row(row):
    ret = False
    for field in row:
        if "INT" in field or "PRIMARY KEY" in field or "AUTO_INCREMENT" in field or \
            "VARCHAR" in field or "DECIMAL" in field or "DATЕ" in field:
            ret = True
    return ret

def create_table(row, table_name):
    for descr in row:
        if "AUTO_INCREMENT" in descr:
            row[row.index(descr)] = descr.replace("AUTO_INCREMENT", "").replace("INT", "SERIAL").strip()
    table_descr = ',\n'.join([" ".join(item) for item in zip(cols, row)])
    cur.execute(f'''DROP TABLE IF EXISTS {table_name};''')
    cur.execute(f'''CREATE TABLE IF NOT EXISTS {table_name} ({table_descr});''')

with open(args.input_file) as inf:
    for line in inf.readlines():
        if html_table and h_line.search(line):
            html_table = False
        if not line.startswith('|'):
            continue
        row = [item.strip() for item in line.split('|')[1:-1]]
        if not cols:
            cols = row[:]
        else:
            out.append(row)

con = psycopg2.connect(
  database="guest_db", 
  user="guest", 
  password="guest", 
  host="192.168.1.55", 
  port="5432"
)

cur = con.cursor()

if html_table and not cols:
    with open(args.input_file) as inf:
        for line in inf.readlines():
            row = [item.strip() for item in line.split(chr(9))]
            if not cols:
                cols = row[:]
            elif not fields_description_row(row):
                out.append(row)
            elif args.create_table:
                create_table(row, cols[0].split('_')[0])

table_name = cols[0].split('_')[0]

columns = ", ".join(cols[1:])
for rec in out:
    values = "', '".join(rec[1:])
    print(f'''INSERT INTO {table_name}({columns}) VALUES ('{values}');''')
    cur.execute(f'''INSERT INTO {table_name}({columns}) VALUES ('{values}');''')
    con.commit()

con.close()
