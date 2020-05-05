# -*- coding: utf-8 -*-
# Mutated from code examples for Aerospike Modeling: IoT Sensors
#   https://dev.to/aerospike/aerospike-modeling-iot-sensors-453a
#   https://github.com/aerospike-examples/modeling-iot-sensors
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
    print("\nGet sensor1 data for April 2nd")
    key = (options.namespace, options.set, "sensor1-04-02")
    pause()
    _, _, b = client.get(key)
    pp.pprint(b["t"])
    print(spacer)

    print("\nGet a four day range for sensor1")
    pause()
    dt = datetime.datetime(2018, 9, 1, 0, 0, 0)
    keys = []
    for i in range(1, 5):
        keys.append(
            (
                options.namespace,
                options.set,
                "sensor1-{:02d}-{:02d}".format(dt.month, dt.day),
            )
        )
        dt = dt + timedelta(days=1)
    sensor_year = client.get_many(keys)
    for rec in sensor_year:
        k, _, b = rec
        print(k[2])
        pause()
        pp.pprint(b["t"])
        print(spacer)
except ex.RecordError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
client.close()
