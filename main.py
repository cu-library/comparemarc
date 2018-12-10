#! /usr/bin/env python3

import click
import pymarc
import psycopg2
import multiprocessing
import time
import mmap
import random
import string
import config

@click.group()
def cli():
    pass

@cli.command()
@click.option("--inputmarc", type=click.Path(exists=True, dir_okay=False, resolve_path=True, readable=True))
@click.option("--records", default=0, help="Number of records in the input, use to skip counting records.")
@click.option('--delete/--no-delete', default=True, help="Delete the marcdata table before loading.")
def load(inputmarc, records, delete):
    """Load a marc file into the database."""
    click.echo("Loading MARC data into database.")

    if delete:
        click.echo("Deleting 'marcdata' table...", nl=False)
        conn = psycopg2.connect(f"dbname={config.database} user={config.username} password={config.password}")
        cur = conn.cursor()
        cur.execute("TRUNCATE marcdata")
        conn.commit()
        cur.close()
        conn.close()
        click.echo(" done.")

    numrecords = 0
    if records == 0:
        click.echo("Counting records in input file...", nl=False)
        with open(inputmarc, 'rb') as f:
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
        p = multiprocessing.Process(target=loader, args=(q,i))
        processes.append(p)
        p.start()

    numprocessed = 0
    with open(inputmarc, 'rb') as fh:
        reader = pymarc.MARCReader(fh, to_unicode=True)
        with click.progressbar(reader,
                       label='Processing MARC records',
                       length=numrecords) as reader:
            for record in reader:
                q.put(record)
                while q.qsize() > 100:
                    time.sleep(1)
                numprocessed+=1
        for process in processes:
            q.put(None)
        for process in processes:
            process.join()

    click.echo("Done, processed {} records.".format(numprocessed))

def loader(inputqueue, id):
    num = 0
    conn = psycopg2.connect(f"dbname={config.database} user={config.username} password={config.password}")
    cur = conn.cursor()
    for record in iter(inputqueue.get, None):
        bibid = record.get_fields('001')[0].value()
        for field in record.get_fields():
            if field.tag != '001':
                subfields = getattr(field, 'subfields', [" ", field.value()])
                for subfield, value in zip(subfields[0::2], subfields[1::2]):
                    cur.execute("INSERT INTO marcdata (bibid, tag, indicator1, indicator2, subfield, value) VALUES (%s, %s, %s, %s, %s, %s)",
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

    click.echo("Deleting rows from the 'marcdata' table...", nl=False)
    cur.execute("DELETE FROM marcdata WHERE (bibid, tag, indicator1, indicator2, subfield, value) IN (SELECT bibid, tag, indicator1, indicator2, subfield, value FROM marcdata TABLESAMPLE BERNOULLI (%s))", (delete,))
    conn.commit()
    click.echo(" done. [{}]".format(cur.rowcount))

    click.echo("Changing rows from the 'marcdata' table...", nl=False)
    cur.execute("UPDATE marcdata SET value = md5(random()::text) WHERE (bibid, tag, indicator1, indicator2, subfield, value) IN (SELECT bibid, tag, indicator1, indicator2, subfield, value FROM marcdata TABLESAMPLE BERNOULLI (%s))",
                        (change,))
    conn.commit()
    click.echo(" done. [{}]".format(cur.rowcount))

    cur.close()
    conn.close()


@cli.command()
def check():
    """Check a marc file against a database to ensure no unexpected changes occurred."""
    click.echo('check')

if __name__ == '__main__':
    cli()
