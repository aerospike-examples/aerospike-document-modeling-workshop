# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception as ex
from aerospike_helpers.operations import list_operations as lh
import sys


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

sensor_id = 1
spacer = "=" * 30
minute = 0
f = open("./sensor.csv", "r")
for line in f:
    try:
        try:
            _, h, t = line.split(",")
            try:
                prev_hour
            except:
                prev_temp = int(float(t.strip()[1:-1]) * 10)
                prev_day = h[1:6]
                prev_hour = int(h[7:9])
                continue
            temp = int(float(t.strip()[1:-1]) * 10)
            day = h[1:6]
            hour = int(h[7:9])
            readings = []
            step = (temp - prev_temp) / 60.0
            for i in range(0, 60):
                prev_temp = prev_temp + step
                readings.append([minute, int(prev_temp)])
                minute = minute + 1
            key = (options.namespace, options.set, "sensor{}-{}".format(sensor_id, prev_day))
            print(spacer)
            print("Day {0} hour {1}".format(prev_day, prev_hour))
            print(readings)
            client.operate(key, [lh.list_append_items("t", readings)], policy=policy)
            if day != prev_day:
                minute = 0
            prev_temp = temp
            prev_day = day
            prev_hour = hour
        except ValueError as e:
            print(e)
            pass
    except IndexError:
        pass
f.close()

print(spacer)
print("Sensor {} data for December 31".format(sensor_id))
k, m, b = client.get((options.namespace, options.set, "sensor{}-12-31".format(sensor_id)))
print(b)
print("Above is Sensor {} data for December 31".format(sensor_id))
print(spacer)
client.close()
