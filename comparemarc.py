#! /usr/bin/env python3
"""
comparemarc - Load a MARC file into a PostgreSQL database, then compare it to another MARC file.
"""

import click
import pymarc
import psycopg2
from psycopg2 import sql
import psycopg2.extras
import multiprocessing
import time
import mmap
import random
import string
import config
import sys
from operator import itemgetter
from tabulate import tabulate

@click.group()
def cli():
    pass

@cli.command()
@click.option('--records', default=0, help="Number of records in the input, use to skip counting records.")
@click.option('--delete/--no-delete', default=True, help=f"Delete the {config.table} table before loading.")
@click.option('--bibidselector', default='001', help="Where is the unique bibid stored. ex: 001 or 907a")
@click.option('--trimbibid/--no-trimbibid', default=True, help="Delete the last character in the bibid.")
@click.argument('inputfile', type=click.Path(exists=True, dir_okay=False, resolve_path=True, readable=True))
def load(records, delete, bibidselector, trimbibid, inputfile):
    """Load a MARC file into the database."""

    if delete:
        click.echo(f"Deleting '{config.table}' table...", nl=False)
        conn = psycopg2.connect(f"dbname={config.database} user={config.username} password={config.password}")
        cur = conn.cursor()
        cur.execute(sql.SQL("TRUNCATE {}").format(sql.Identifier(config.table)))
        conn.commit()
        cur.close()
        conn.close()
        click.echo(" done.")

    numrecords = 0
    if records == 0:
        numrecords = countrecords(inputfile)
    else:
        numrecords = records

    # Start up the subprocesses which will load the data into the database.
    # The subprocesses use a queue to receive the records from the parent process.
    q = multiprocessing.Queue()
    processes = []
    # Because the parent process sleeps, we can afford to have an additional subprocess.
    for i in range(multiprocessing.cpu_count()+1):
        p = multiprocessing.Process(target=loader, args=(q, bibidselector, trimbibid))
        processes.append(p)
        p.start()

    # Run through the input file and add each record to the queue.
    numprocessed = 0
    with open(inputfile, 'rb') as f:
        reader = pymarc.MARCReader(f, to_unicode=True, force_utf8=True)
        with click.progressbar(reader,
                       label='Processing MARC records',
                       length=numrecords) as reader:
            for record in reader:
                # This checks that the bibID check doesn't fail in the subprocess.
                getBibID(record, bibidselector, trimbibid)
                q.put(record)
                # If the 'reader' falls to far behind the 'loaders', slow down a bit, jeez.
                # This is here to keep the memory usage of the script down. Otherwise the queue might
                # grow to the size of the input file.
                while q.qsize() > 100:
                    time.sleep(1)
                numprocessed+=1

        # After processing, add None to the queue to flag that the subprocesses can stop.
        for process in processes:
            q.put(None)
        for process in processes:
            process.join()

    click.echo("Done, processed {} records.".format(numprocessed))

def loader(inputqueue, bibidselector, trimbibid):
    num = 0
    conn = psycopg2.connect(f"dbname={config.database} user={config.username} password={config.password}")
    cur = conn.cursor()
    insert = sql.SQL("INSERT INTO {} (bibid, tag, indicator1, indicator2, subfield, value) VALUES %s").format(sql.Identifier(config.table))
    valuestoinsert = []
    for record in iter(inputqueue.get, None):
        bibid = getBibID(record, bibidselector, trimbibid)
        for field in record.get_fields():
            subfields = getattr(field, 'subfields', [" ", field.value()])
            for subfield, value in zip(subfields[0::2], subfields[1::2]):
                valuestoinsert.append((bibid, field.tag, getattr(field, 'indicator1', ""), getattr(field, 'indicator2', ""), subfield, value))
        num+=1
        if num % 1000 == 0:
            psycopg2.extras.execute_values(cur, insert, valuestoinsert)
            valuestoinsert = []
            conn.commit()

    psycopg2.extras.execute_values(cur, insert, valuestoinsert)
    conn.commit()
    cur.close()
    conn.close()

@cli.command()
@click.option("--delete", default=10, help="Percent of rows in the table to delete.")
@click.option("--change", default=10, help="Percent of rows in the database to change the value of.")
@click.option("--add", default=10, help="Percent of rows in the database to copy, modify, and add.")
def gremlin(delete, change, add):
    """Unleash a gremlin into the database to make changes."""

    conn = psycopg2.connect(f"dbname={config.database} user={config.username} password={config.password}")
    cur = conn.cursor()

    click.echo(f"Deleting rows from the '{config.table}' table...", nl=False)
    cur.execute(sql.SQL("DELETE FROM {} WHERE (bibid, tag, indicator1, indicator2, subfield, value) IN (SELECT bibid, tag, indicator1, indicator2, subfield, value FROM {} TABLESAMPLE BERNOULLI (%s))").format(sql.Identifier(config.table),sql.Identifier(config.table)),
                             (delete,))
    conn.commit()
    click.echo(f" done. [{cur.rowcount}]")

    click.echo(f"Changing rows in the '{config.table}' table...", nl=False)
    cur.execute(sql.SQL("UPDATE {} SET value = md5(random()::text) WHERE (bibid, tag, indicator1, indicator2, subfield, value) IN (SELECT bibid, tag, indicator1, indicator2, subfield, value FROM {} TABLESAMPLE BERNOULLI (%s))").format(sql.Identifier(config.table), sql.Identifier(config.table)),
                        (change,))
    conn.commit()
    click.echo(f" done. [{cur.rowcount}]")

    click.echo(f"Adding new rows to the '{config.table}' table...", nl=False)
    readcur = conn.cursor()
    readcur.execute(sql.SQL("SELECT bibid, tag, indicator1, indicator2, subfield, value FROM {} TABLESAMPLE BERNOULLI (%s)").format(sql.Identifier(config.table)), (add,))
    for row in readcur:
        rowvalues = list(row)
        randomindex = random.randrange(1, len(rowvalues))
        rowvalues[randomindex] = "".join(random.choice(string.ascii_letters+string.digits) for i in range(random.randint(6,20)))
        cur.execute(sql.SQL("INSERT INTO {} (bibid, tag, indicator1, indicator2, subfield, value) VALUES (%s, %s, %s, %s, %s, %s)").format(sql.Identifier(config.table)),
                                 rowvalues)
    conn.commit()
    click.echo(f" done. [{readcur.rowcount}]")

    cur.close()
    conn.close()


@cli.command()
@click.option('--records', default=0, help="Number of records in the input, use to skip counting records.")
@click.option('--bibidselector', default='001', help="Where is the unique bibid stored. ex: 001 or 907a")
@click.option('--trimbibid/--no-trimbibid', default=True, help="Delete the last character in the bibid.")
@click.option('--unchanged/--no-unchanged', default=True, help="Include unchanged records in the report.")
@click.option('--ignore', multiple=True, help="Changes in these tags will be ignored.")
@click.argument('inputfile', type=click.Path(exists=True, dir_okay=False, resolve_path=True, readable=True))
@click.argument('outputfile', type=click.Path(resolve_path=True))
def check(records, bibidselector, trimbibid, unchanged, ignore, inputfile, outputfile):
    """Check a MARC file against the database to ensure no unexpected changes occurred."""

    numrecords = 0
    if records == 0:
        numrecords = countrecords(inputfile)
    else:
        numrecords = records

    q = multiprocessing.Queue()
    printq = multiprocessing.Queue()
    processes = []
    for i in range(multiprocessing.cpu_count()):
        p = multiprocessing.Process(target=compare, args=(q, bibidselector, printq, trimbibid, ignore))
        processes.append(p)
        p.start()

    printer = multiprocessing.Process(target=writefromqueue, args=(printq, outputfile, unchanged, ignore))
    printer.start()

    numprocessed = 0
    with open(inputfile, 'rb') as f:
        reader = pymarc.MARCReader(f, to_unicode=True, force_utf8=True)
        with click.progressbar(reader,
                       label='Processing MARC records',
                       length=numrecords) as reader:
            for record in reader:
                getBibID(record, bibidselector, trimbibid)
                q.put(record)
                while q.qsize() > 100:
                    time.sleep(1)
                numprocessed+=1
        for process in processes:
            q.put(None)
        for process in processes:
            process.join()

    printq.put(None)
    printer.join()

    click.echo("Done, processed {} records.".format(numprocessed))

def compare(inputqueue, bibidselector, printq, trimbibid, ignore):
    num = 0
    conn = psycopg2.connect(f"dbname={config.database} user={config.username} password={config.password}")
    for record in iter(inputqueue.get, None):
        bibid = getBibID(record, bibidselector, trimbibid)
        setofrowsdb = set()
        setofrowsfile = set()
        readcur = conn.cursor()
        readcur.execute(sql.SQL("SELECT bibid, tag, indicator1, indicator2, subfield, value FROM {} WHERE bibid = %s").format(sql.Identifier(config.table)), (bibid,))
        for row in readcur:
            if row[1] not in ignore:
                setofrowsdb.add(row)

        for field in record.get_fields():
            subfields = getattr(field, 'subfields', [" ", field.value()])
            for subfield, value in zip(subfields[0::2], subfields[1::2]):
                if field.tag not in ignore:
                    setofrowsfile.add((bibid, field.tag, getattr(field, 'indicator1', ""), getattr(field, 'indicator2', ""), subfield, value))

        dbonly = set()
        dbminusfile = setofrowsdb.difference(setofrowsfile)
        for row in dbminusfile:
            dbonly.add(f"{row[1]},{row[2]},{row[3]},{row[4]}")

        fileonly = set()
        fileminusdb = setofrowsfile.difference(setofrowsdb)
        for row in fileminusdb:
            fileonly.add(f"{row[1]},{row[2]},{row[3]},{row[4]}")

        printq.put((bibid, dbminusfile, fileminusdb, dbonly, fileonly))

    conn.close()

def writefromqueue(printq, outputfile, unchanged, ignore):

    dbonly = set()
    fileonly = set()
    changed = 0

    with open(outputfile, 'w', encoding='utf-8') as f:

        if len(ignore) > 0:
             f.write(f"Ignoring fields {', '.join(sorted(ignore))}.\n\n\n")

        for bibidvalue, dbminusfile, fileminusdb, newdbonly, newfileonly in iter(printq.get, None):
            dbonly.update(newdbonly)
            fileonly.update(newfileonly)

            dbminusfilelen = len(dbminusfile)
            fileminusdblen = len(fileminusdb)

            if dbminusfilelen > 0 or fileminusdblen > 0:
                changed+=1
                f.write(f"Changes in {bibidvalue}:\n\n")

                if dbminusfilelen != 0:
                    sorteddbminusfile = sorted(list(dbminusfile), key=itemgetter(1,2,3,4,5))
                    sorteddbminusfile = [(x[0], x[1], x[2], x[3], x[4], f"`{x[5]}`") for x in sorteddbminusfile]
                    f.write("In database not in file:\n")
                    f.write(tabulate(sorteddbminusfile, headers=["bibid", "tag", "i1", "i2", "subf", "`value`"]))
                    f.write("\n\n")

                if fileminusdblen != 0:
                    sortedfileminusdb = sorted(list(fileminusdb), key=itemgetter(1,2,3,4,5))
                    sortedfileminusdb = [(x[0], x[1], x[2], x[3], x[4], f"`{x[5]}`") for x in sortedfileminusdb]
                    f.write("In file not in database:\n")
                    f.write(tabulate(sortedfileminusdb, headers=["bibid", "tag", "i1", "i2", "subf", "`value`"]))
                    f.write("\n\n")

                f.write("\n")

            elif unchanged:
                f.write(f"No changes in {bibidvalue}.\n\n\n")

        sorteddbonly = sorted(list(dbonly))
        sortedfileonly = sorted(list(fileonly))

        f.write("Summary:\n")
        f.write(f"  {changed} records have been changed.\n")
        if len(sorteddbonly) > 0:
            f.write("  MARC elements found in the database, not in the file:\n")
            for elem in sorteddbonly:
                f.write(f"    {elem}\n")
        if len(sortedfileonly) > 0:
            f.write("  MARC elements found in the file, not in the database:\n")
            for elem in sortedfileonly:
                f.write(f"    {elem}\n")

def getBibID(record, bibidselector, trimbibid):
    bibid = None
    if len(bibidselector) == 4:
        bibid = record[bibidselector[0:3]][bibidselector[3]]
    else:
        bibid = record[bibidselector].value()

    if bibid == None:
        click.echo(record.as_dict())
        sys.exit(1)
    else:
        if trimbibid:
            bibid = bibid[:-1]
        return bibid

def countrecords(inputfile):
    numrecords = 0
    click.echo("Counting records in input file...", nl=False)
    with open(inputfile, 'rb') as f:
        mm = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
        while True:
            cur = mm.read(1)
            if cur:
                if cur == b'\x1d':
                    numrecords+=1
            else:
                break
    click.echo(f" done. [{numrecords}]")
    return numrecords

if __name__ == '__main__':
    cli()
