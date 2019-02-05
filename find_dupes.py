#!/usr/bin/env python3
# Kyle Fitzsimmons, 2018
import csv
import os
import time

fn = 'output-coordinates.csv'

filesize = os.path.getsize(fn)

start = time.time()
seen = set()
num_uniques = 0
duplicates = []
num_duplicates = 0

last_status_pct = 0
pct_interval = 5

with open(fn, 'rb') as csv_f:
    headers = csv_f.readline()

    for row in csv_f:
        bytes_read = csv_f.tell()
        row_hash = hash(row)
        if row_hash not in seen:
            seen.add(row_hash)
            num_uniques += 1
        else:
            duplicates.append(row)
            num_duplicates += 1

        progress = (float(bytes_read) / filesize) * 100
        if progress > last_status_pct:
            print('Processing: {pct:.2f}%'.format(pct=progress))
            last_status_pct += pct_interval
    
print('Uniques: {u} / Duplicates: {d}'.format(u=num_uniques, d=num_duplicates))
print(duplicates)
