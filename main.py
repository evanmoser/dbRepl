import os
import logging
from lib.db import Db
import lib.data as data

fn = os.path.join('log', 'output.log')

logging.basicConfig(filename=fn, filemode='w', level=logging.INFO, format='%(asctime)s: %(message)s', datefmt=None)

logging.info("Database replication started.")

#Establish the table and primary key. Used for comparison and queries.
table = "table_name"
pk = "primary_key"

#Create database objects. Pass DSN of ODBC connection as an argument.
db01 = Db("mssql_source_db_DSN")
db02 = Db("mysql_destination_db_DSN")

#Update/Insert records in destination DB that do not match source DB.
db01_rows = db01.qfa("SELECT * FROM {0}".format(table))
db01_hdrs = db01.getHeaders();

db02_rows = db02.qfa("SELECT * FROM {0}".format(table))
db02_hdrs = db02.getHeaders();

db01_results = list()
db02_results = list()

for db01_row in db01_rows:
    db01_results.append(data.getDict(db01_hdrs, db01_row))

for db02_row in db02_rows:
    db02_results.append(data.getDict(db02_hdrs, db02_row))

logging.info("Preparing to insert/update records.")
for dict in db01_results:
    subject = dict[pk]

    try:
        find = next(item for item in db02_results if item[pk] == "{0}".format(subject))
    except:
        find = None
        logging.debug("{0} record not found in destination database. Setting variable to none.")

    if dict != find:
        query = data.repQuery(dict, find, table, pk)
        try:
            db02.query(query[0])
            logging.info(query[1])
        except Exception as e:
            logging.debug("{0} failed to update/insert with the following error: {1}".format(subject, e))
    else:
        logging.debug("{0} matches destination database.".format(subject))

logging.info("Insert/update records complete.")

del db01_results[:]
del db02_results[:]

#Delete records from destination DB that no longer exist in source DB.
logging.info("Preparing to delete records.")
db01_count = db01.qfo("SELECT COUNT({0}) FROM {1}".format(pk, table))[0]
db02_count = db02.qfo("SELECT COUNT({0}) FROM {1}".format(pk, table))[0]

if db02_count > db01_count:
    db01_pk = db01.qfa("SELECT {0} FROM {1}".format(pk, table))
    db02_pk = db02.qfa("SELECT {0} FROM {1}".format(pk, table))

    db01_pks_todelete = data.listToString(db01_pk)

    db02_diff = db02.qfa("SELECT {0} FROM {1} WHERE {2} NOT IN ({3})".format(pk, table, pk, db01_pks_todelete))

    for todelete in db02_diff:
        query = data.delQuery(data.dataToString(todelete[0]), pk, table)
        try:
            db02.query(query[0])
            logging.info(query[1])
        except Exception as e:
            logging.debug("{0} failed to delete with the following error: {1}".format(todelete[0], e))
else:
    logging.info("No records to delete.")

logging.info("Delete records complete.")

db01.disconnect()
db02.disconnect()

logging.info("Database replication complete.")
