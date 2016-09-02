#! /usr/bin/etc python
# -*- coding: utf-8 -*-

__author__ = 'Gao Ming'
import time
import datetime


def product_events(records):
    """
    From the file read every taxi start and end record. For each record product the following events:
     1. The passenger start request
     2. The passenger end request
     3. The driver start supplying
     4. The driver end supplying
     5. The driver send speed.

    The start and end of passenger and driver behavior according to the config file.

    """
    for record in records:
        record = record.replace("\n", "")
        fields = record.split(",")
        trip_start_time = fields[1]
        ts_d = datetime.datetime.strptime(trip_start_time, "%Y-%m-%d %H:%M:%S")
        ts_d.astimezone()
        ts_ts = time.mktime(ts_d.timetuple())
        trip_end_time = fields[2]
        pick_up_long = fields[5]
        pick_up_la = fields[6]
        drop_off_long = fields[7]
        drop_off_la = fields[8]
    print "Helloworld"
