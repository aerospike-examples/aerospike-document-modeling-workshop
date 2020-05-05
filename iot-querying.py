# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception as ex
from aerospike_helpers.operations import list_operations as lh
from aerospike import predexp as pxp
import datetime
from datetime import timedelta
import pprint
import sys

def pause():
    input("Hit return to continue")


def print_sensor_data(rec):
    k, _, b = rec
    print(k[2])
    print(b['t'])
    print("=" * 30)


if options.set == "None":
    options.set = None
config = {"hosts": [(options.host, options.port)]}
if options.alternate:
    config["use_services_alternate"] = True
try:
    client = aerospike.client(config).connect(options.username, options.password)
    policy = {"key": aerospike.POLICY_KEY_SEND}
except ex.ClientError as e:
    if not options.quiet:
        print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(2)

pp = pprint.PrettyPrinter(indent=2)
spacer = "=" * 30
try:
    key = (options.namespace, options.set, "sensor1-12-31")
    print("\nRetrieve sensor1 data for 9-11am, December 31st")
    pause()
    starts = 9 * 60
    ends = 11 * 60
    ops = [
        lh.list_get_by_value_range(
            "t",
            aerospike.LIST_RETURN_VALUE,
            [starts, aerospike.null()],
            [ends, aerospike.null()],
        )
    ]
    _, _, b = client.operate(key, ops)
    pp.pprint(b["t"])
    print(spacer)

    print("\nScan for a random sampling (about 1%) of all the sensor data")
    pause()
    predexp =  [
        pxp.rec_digest_modulo(120),
        pxp.integer_value(1),
        pxp.integer_equal()
    ]
    query = client.query(options.namespace, options.set)
    query.predexp(predexp)
    query.foreach(print_sensor_data)
except ex.RecordError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
client.close()
