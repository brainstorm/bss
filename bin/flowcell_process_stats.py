#!/usr/bin/env python

""" Flowcell Writer Replay

    Given a UNIX "stat" input generated by:
        find . | xargs stat "%s %n %y" > flowcell_filesizes.tsv

    With contents such as:

    2402705 ./Data/Intentities/BaseCalls/L005/C171.1/s_5_2219.bcl.gz 2015-10-07 02:00:40.454831539 +0100
    2453492 ./Data/Intentities/BaseCalls/L005/C171.1/s_5_2120.bcl.gz 2015-10-07 02:00:29.917686120 +0100
    2477752 ./Data/Intentities/BaseCalls/L005/C171.1/s_5_1203.bcl.gz 2015-10-07 01:57:34.184586979 +0100
    (...)

    This script writes the directory and file structure to (stress) test
    filesystem event processing systems like:

    https://github.com/axkibe/lsyncd
    https://github.com/brainstorm/bss

    The file contents are based on /dev/urandom for now, matching the filesize in the first column of the
    TSV file.

    I.e:
        /flowcell_writes_replay.py -f virtual_fc -s flowcell_filesizes_bcls.tsv.gz -r 1000

    NOTE: The resulting directory/file structure can easily fill up your storage.
"""
import pandas as pd
import os
from time import sleep
import click
import logging
import re

logging.basicConfig(
        format="%(asctime)s %(levelname)-5s %(name)s : %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO)

log = logging.getLogger("proc-stats")

ILLUMINA_FILESIZES='flowcell_filesizes.tsv.gz'

@click.command()
@click.argument('stats_path', type=click.Path(exists=True))
@click.option('-o', '--output', 'events_path', default="events.tsv", help="Output file for the events")
def main(stats_path, events_path):
    # y[0:15] is clipping the last 3 digits of the microseconds since
    # https://docs.python.org/2/library/datetime.html#strftime-strptime-behavior
    # states that: "the %f directive accepts from one to six digits and zero pads
    # on the right... stat returned 9 digits instead of 6"

    dateparse = lambda x,y: pd.datetime.strptime(x + " " + y[0:15], '%Y-%m-%d %H:%M:%S.%f')

    log.info("Loading stats from {} ... ".format(stats_path))

    stats = pd.read_table(stats_path, parse_dates={'datetime' : ["date", "time"]},
                          index_col='datetime', date_parser = dateparse, sep=' ',
                          names=['size', 'filename', 'date', 'time', 'offset'])

    del stats['offset']

    stats = stats[~stats["filename"].str.contains("Thumbnail", flags=re.IGNORECASE)]

    log.info("Processing stats ...")

    # Order timeseries and calculate deltas between file creation
    stats.sort_index(ascending=True, inplace=True)
    deltas = stats.index.to_series().diff().values
    deltas[0:-1] = deltas[1:]
    deltas[-1] = 0
    stats['deltas'] = deltas.astype('timedelta64[ns]').astype('int')

    log.info("Saving events into {} ...".format(events_path))

    stats.to_csv(events_path, sep="\t")

if __name__ == '__main__':
    main()
