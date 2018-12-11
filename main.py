#! /usr/bin/env python3

import click
import pymarc
import psycopg2
from psycopg2 import sql
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
@click.option('--bibidselector', default='001', help=f"Where is the unique bibid stored. ex: 001 or 907a")
@click.option('--trimbibid/--no-trimbibid', default=True, help=f"Delete the last character in the bibid.")
@click.argument('input', type=click.Path(exists=True, dir_okay=False, resolve_path=True, readable=True))
def load(records, delete, bibidselector, trimbibid, input):
    """Load a marc file into the database."""
    click.echo("Loading MARC data into database.")

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
        click.echo("Counting records in input file...", nl=False)
        with open(input, 'rb') as f:
            mm = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
            while True:
                cur = mm.read(1)
                if cur:
                    if cur == b'\x1d':
                        numrecords+=1
                else:
                    break
        click.echo(" done.")
    else:
        numrecords = records

    q = multiprocessing.Queue()
    processes = []
    for i in range(multiprocessing.cpu_count()):
        p = multiprocessing.Process(target=loader, args=(q, bibidselector, trimbibid))
        processes.append(p)
        p.start()

    numprocessed = 0
    with open(input, 'rb') as f:
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

    click.echo("Done, processed {} records.".format(numprocessed))

def loader(inputqueue, bibidselector, trimbibid):
    num = 0
    conn = psycopg2.connect(f"dbname={config.database} user={config.username} password={config.password}")
    cur = conn.cursor()
    for record in iter(inputqueue.get, None):
        bibid = getBibID(record, bibidselector, trimbibid)
        for field in record.get_fields():
            subfields = getattr(field, 'subfields', [" ", field.value()])
            for subfield, value in zip(subfields[0::2], subfields[1::2]):
                cur.execute(sql.SQL("INSERT INTO {} (bibid, tag, indicator1, indicator2, subfield, value) VALUES (%s, %s, %s, %s, %s, %s)").format(sql.Identifier(config.table)),
                             (bibid, field.tag, getattr(field, 'indicator1', ""), getattr(field, 'indicator2', ""), subfield, value))
        num+=1
        if num % 1000 == 0:
            conn.commit()

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
@click.option('--bibidselector', default='001', help=f"Where is the unique bibid stored. ex: 001 or 907a")
@click.option('--trimbibid/--no-trimbibid', default=True, help=f"Delete the last character in the bibid.")
@click.argument('input', type=click.Path(exists=True, dir_okay=False, resolve_path=True, readable=True))
def check(records, bibidselector, trimbibid, input):
    """Check a marc file against a database to ensure no unexpected changes occurred."""
    click.echo("Checking MARC data against database.")

    numrecords = 0
    if records == 0:
        click.echo("Counting records in input file...", nl=False)
        with open(input, 'rb') as f:
            mm = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
            while True:
                cur = mm.read(1)
                if cur:
                    if cur == b'\x1d':
                        numrecords+=1
                else:
                    break
        click.echo(" done.")
    else:
        numrecords = records

    q = multiprocessing.Queue()
    printq = multiprocessing.Queue()
    processes = []
    for i in range(multiprocessing.cpu_count()-1):
        p = multiprocessing.Process(target=compare, args=(q, bibidselector, printq, trimbibid))
        processes.append(p)
        p.start()

    printer = multiprocessing.Process(target=printfromqueue, args=(printq,))
    printer.start()

    numprocessed = 0
    with open(input, 'rb') as f:
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

def compare(inputqueue, bibidselector, printq, trimbibid):
    num = 0
    conn = psycopg2.connect(f"dbname={config.database} user={config.username} password={config.password}")
    for record in iter(inputqueue.get, None):
        bibid = getBibID(record, bibidselector, trimbibid)
        setofrowsdb = set()
        setofrowsfile = set()
        readcur = conn.cursor()
        readcur.execute(sql.SQL("SELECT bibid, tag, indicator1, indicator2, subfield, value FROM {} WHERE bibid = %s").format(sql.Identifier(config.table)), (bibid,))
        for row in readcur:
            setofrowsdb.add(row)

        for field in record.get_fields():
            subfields = getattr(field, 'subfields', [" ", field.value()])
            for subfield, value in zip(subfields[0::2], subfields[1::2]):
                setofrowsfile.add((bibid, field.tag, getattr(field, 'indicator1', ""), getattr(field, 'indicator2', ""), subfield, value))

        printq.put((bibid, setofrowsdb, setofrowsfile))

    conn.close()

def printfromqueue(queue):
    for bibidvalue, setofrowsdb, setofrowsfile in iter(queue.get, None):
        click.echo(f"----Report for {bibidvalue}----\n")

        dbminusfile = setofrowsdb.difference(setofrowsfile)
        if len(dbminusfile) != 0:
            sorteddbminusfile = sorted(list(dbminusfile), key=itemgetter(1,2,3,4,5))
            click.echo("In database not in file:")
            click.echo(tabulate(sorteddbminusfile, headers=["bibid", "tag", "indicator1", "indicator2", "subfield", "value"]))
            click.echo("")

        fileminusdb = setofrowsfile.difference(setofrowsdb)
        if len(fileminusdb) != 0:
            sortedfileminusdb = sorted(list(fileminusdb), key=itemgetter(1,2,3,4,5))
            click.echo("In file not in database:")
            click.echo(tabulate(sortedfileminusdb, headers=["bibid", "tag", "indicator1", "indicator2", "subfield", "value"]))
            click.echo("")

def getBibID(record, bibidselector, trimbibid):
    bibid = None
    if len(bibidselector) == 4:
        bibid = record[bibidselector[0:3]][bibidselector[3]]
    else:
        bibid = record[bibidselector]

    if bibid == None:
        click.echo(record.as_dict())
        sys.exit(1)
    else:
        if trimbibid:
            bibid = bibid[:-1]
        return bibid

if __name__ == '__main__':
    cli()
