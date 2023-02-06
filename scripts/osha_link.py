#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This code demonstrates how to use dedupe with a comma separated values
(CSV) file. All operations are performed in memory, so will run very
quickly on datasets up to ~10,000 rows.

We start with a CSV file containing our messy data. In this example,
it is listings of early childhood education centers in Chicago
compiled from several different sources.

The output will be a CSV with our clustered results.

For larger datasets, see our [mysql_example](mysql_example.html)
"""

import os
import csv
import logging

import dedupe


def preProcess(column):
    """
    Do a little bit of data cleaning with the help of Unidecode and Regex.
    Things like casing, extra spaces, quotes and new lines can be ignored.
    """
    column = column.lower()
    # If data is missing, indicate that by setting the value to `None`
    if not column:
        column = None
    return column


def readData(filename, identifier):
    """
    Read in our data from a CSV file and create a dictionary of records,
    where the key is a unique record ID and each value is dict
    """

    data_d = {}
    with open(filename) as f:
        reader = csv.DictReader(f)
        for row in reader:
            clean_row = [
                (k, None if k == "naic_cd" and v == "0" else preProcess(v))
                for (k, v) in row.items()
            ]
            row_id = int(row[identifier])
            data_d[row_id] = dict(clean_row)

    return data_d


if __name__ == "__main__":
    import sys

    # ## Logging
    logging.basicConfig(level=logging.DEBUG)

    # ## Setup

    input_file_1 = sys.argv[1]
    input_file_2 = sys.argv[2]
    settings_file = "establishment/learned_settings"
    training_file_in = "whd_training.json"
    training_file_out = "whd_osha_training.json"

    print("importing data ...")
    data_1 = readData(input_file_1, "activity_nr")
    data_2 = readData(input_file_2, "case_id")

    # ## Training

    # Define the fields dedupe will pay attention to
    fields = [
        {"field": "trade_nm", "type": "String"},
        {"field": "legal_name", "type": "String", "has missing": True},
        {"field": "street_addr_1_txt", "type": "String", "has missing": True},
        {"field": "cty_nm", "type": "String"},
        {"field": "st_cd", "type": "Exact"},
        {"field": "st_cd", "type": "String"},
        {"field": "naic_cd", "type": "String", "has missing": True},
    ]

    # Create a new linker object and pass our data model to it.
    linker = dedupe.Gazetteer(fields)

    # If we have training data saved from a previous run of dedupe,
    # look for it and load it in.
    # __Note:__ if you want to train from scratch, delete the training_file
    if os.path.exists(training_file_out):
        print("reading labeled examples from ", training_file_out)
        with open(training_file_out, "rb") as f:
            linker.prepare_training(data_1, data_2, f)
    elif os.path.exists(training_file_in):
        print("reading labeled examples from ", training_file_in)
        with open(training_file_in, "rb") as f:
            linker.prepare_training(data_1, data_2, f)
    else:
        linker.prepare_training(data_1, data_2)

    # ## Active learning
    # Dedupe will find the next pair of records
    # it is least certain about and ask you to label them as duplicates
    # or not.
    # use 'y', 'n' and 'u' keys to flag duplicates
    # press 'f' when you are finished
    print("starting active labeling...")

    dedupe.console_label(linker)

    # Using the examples we just labeled, train the linker and learn
    # blocking predicates
    linker.train(index_predicates=False)

    # When finished, save our training to disk
    with open(training_file_out, "w") as tf:
        linker.write_training(tf)

    # Save our weights and predicates to disk.  If the settings file
    # exists, we will skip all the training and learning next time we run
    # this file.
    with open(settings_file, "wb") as sf:
        linker.write_settings(sf)
