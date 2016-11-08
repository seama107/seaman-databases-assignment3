#! /usr/bin/env python

"""
File: DataGeneration.py
Copyright (c) 2016 Michael Seaman
License: MIT
Description: Generates random sales data to be entered into a database
1st argument is the number of generated tuples
Default 20,000
Sales data follows the format:
datetime, street address, zip, city, state, item id, item name, item manufacturer, price, lastname, first name, homePhone, cellPhone
"""

import sys
import random
import datetime
import csv

try:
    from faker import Faker
    fake = Faker()
except ImportError:
    print "Unable to generate data - install the package 'faker'."
    sys.exit()


def createPerson():
    """
    Returns a 4-tuple with the following format:
    (firstname, lastname, homephone, cellphone)
    where home and cell can be 'null'
    """

    person = (fake.first_name().encode("ascii"),
        fake.last_name().encode("ascii"),
        fake.phone_number().encode("ascii") if random.choice([True, False]) else 'null',
        fake.phone_number().encode("ascii") if random.choice([True, False]) else 'null')
    return person

def createItem():
    """
    Returns a 4-tuple with the following format:
    (item id, item name, manufacturer name, price)
    """

    item = (random.randint(1,1000000000),
        fake.word().encode("ascii") + ' ' + ' '.join(fake.bs().encode("ascii").split(' ')[-2:]),
        fake.company().encode("ascii"),
        random.randint(1,1000) * .25)
    return item

def createDateTime():
    """
    Returns a datetime object from 2010-2016
    """
    start = datetime.datetime.strptime('01Jan2010', '%d%b%Y')
    end = datetime.datetime.strptime('01Jan2016', '%d%b%Y')
    return fake.date_time_between_dates(datetime_start=start , datetime_end=end)

def createLocation():
    """
    Returns a 4-tuple with the following format:
    (street address, zip, city, state)
    """
    loc = (fake.street_address().encode("ascii"),
        fake.postalcode().encode("ascii"),
        fake.city().encode("ascii"),
        fake.state_abbr().encode("ascii"))

    return loc

def createRecord(possible_locations, possible_items, possible_people, f = sys.stdout):
    """
    Returns a 13-tuple with the following format:
    (datetime, street address, zip, city, state, item id, item name,
    item manufacturer, price, lastname, first name, homePhone, cellPhone)
    Takes 3 parameters, each being long lists of tuples
    """
    datetime = createDateTime()
    loc = random.choice(possible_locations)
    item = random.choice(possible_items)
    person = random.choice(possible_people)
    record = (datetime, loc[0], loc[1], loc[2], loc[3], item[0], item[1], item[2], item[3], person[0], person[1], person[2], person[3])
    writeRecord(record, f)
    return

def writeRecord(record, f = sys.stdout):
    csv.writer(f).writerow(record)
    return


def createNRecords(n, f = sys.stdout):
    n_stores = 10
    n_items = int(n * .04)
    n_people = n
    possible_locations = [createLocation() for x in range(n_stores)]
    possible_items = [createItem() for x in range(n_items)]
    possible_people = [createPerson() for x in range(n_people)]

    for i in range(n):
        createRecord(possible_locations, possible_items, possible_people, f)

    return



num_records = 20000
if len(sys.argv) > 1:
    num_records = int(sys.argv[1])

print "Generating {} records.".format(num_records)

output_file = open("salesData.csv", "w")
createNRecords(num_records, output_file)


#The last line shouldn't have a newline

output_file.close()
