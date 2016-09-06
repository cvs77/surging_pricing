#! /usr/bin/etc python
# -*- coding: utf-8 -*-
# __author__ = 'Gao Ming'
"""
Convert the raw log from file to the driver events and store the events to the database
"""
import datetime
from decimal import *
from hashlib import md5

from geopy.distance import vincenty

from stream_generator.utils.config import DATABASE_CONFIG
from stream_generator.utils.db_helper import StreamDataDbHelper
from stream_generator.utils.config import DRIVER_LASTING, REQUEST_WAITING, DRIVER_HEART_BEAT_INTERVAL


def _convert_date_to_ts(date_str, timezone_offset=-4):
    date_time = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    time_stamp_gmt = (date_time - datetime.datetime(1970, 1, 1)).total_seconds()
    time_stamp_edt = time_stamp_gmt - timezone_offset * 3600
    return time_stamp_edt


def _congestion_heartbeat(trip_start_ts, trip_end_ts, start_lalo, end_lalo):
    trip_duration = trip_end_ts - trip_start_ts
    trip_length = vincenty(start_lalo, end_lalo).miles
    speed = trip_length / trip_duration
    count = int(trip_duration / DRIVER_HEART_BEAT_INTERVAL)
    long_length = end_lalo[1] - start_lalo[1]
    la_length = end_lalo[0] - start_lalo[0]
    new_record = []
    for i in range(count):
        new_long = start_lalo[1] + Decimal(i) / count * long_length
        new_la = start_lalo[0] + Decimal(i) / count * la_length
        new_ts = int(trip_start_ts + float(i) / count * trip_duration)
        new_record.append((new_ts, new_long, new_la, speed))
    return new_record


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
    db_helper = StreamDataDbHelper(DATABASE_CONFIG)
    for record in records:
        record = record.replace("\n", "")
        fields = record.split(",")
        # Get the time information
        trip_start_time = fields[1]
        trip_start_ts = _convert_date_to_ts(trip_start_time)
        trip_end_time = fields[2]
        trip_end_ts = _convert_date_to_ts(trip_end_time)
        # Get the location information
        pick_up_long = Decimal(fields[5])
        pick_up_la = Decimal(fields[6])
        drop_off_long = Decimal(fields[7])
        drop_off_la = Decimal(fields[8])
        # Get the uer hash
        user_id = md5(str(trip_start_ts - REQUEST_WAITING) + fields[5] + fields[6]).hexdigest()
        driver_id = md5(str(trip_start_ts - DRIVER_LASTING) + fields[5] + fields[6]).hexdigest()
        # Generate driver speed heartbeat events
        speed_heartbeat = _congestion_heartbeat(trip_start_ts, trip_end_ts, (pick_up_la, pick_up_long), (drop_off_la, drop_off_long))
        for beat in speed_heartbeat:
            tem = {}
            tem['type'] = "SPEED_HB"
            tem['ts'] = beat[0]
            tem['lo'] = beat[1]
            tem['la'] = beat[2]
            tem['v'] = beat[3]
            tem['uid'] = driver_id
            db_helper.insert(tem)
        # Generate Driver beating data
        # Generate dreiver
        db_helper.insert({
            "type": "NOT_FREE",
            "ts": trip_start_ts,
            "lo": pick_up_long,
            "la": pick_up_la,
            "v": -1,
            "uid": driver_id
        })
        # The driver beating happen before the strip start and after the trip end
        for i in range(DRIVER_LASTING / DRIVER_HEART_BEAT_INTERVAL):
            tem = {'type': "FREE",
                   'ts': trip_start_ts - i * DRIVER_HEART_BEAT_INTERVAL,
                   'lo': pick_up_long,
                   'la': pick_up_la,
                   'v': -1,
                   "uid": driver_id}
            db_helper.insert(tem)
            tem = {'type': "FREE",
                   'ts': trip_end_ts + i * DRIVER_HEART_BEAT_INTERVAL,
                   'lo': drop_off_long,
                   'la': drop_off_la,
                   'v': -1,
                   "uid": driver_id}
            db_helper.insert(tem)
        # Generate User Request and User request End
        db_helper.insert({
            "type": "REQUEST",
            "ts": trip_start_ts - REQUEST_WAITING,
            "lo": pick_up_long,
            "la": pick_up_la,
            "v": -1,
            "uid": user_id
        })
        db_helper.insert({
            "type": "REQUEST_END",
            "ts": trip_start_ts,
            "lo": pick_up_long,
            "la": pick_up_la,
            "v": -1,
            "uid": user_id
        })

