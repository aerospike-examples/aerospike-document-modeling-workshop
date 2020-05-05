# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception as ex
from aerospike_helpers.operations import list_operations as lh
import pprint
import sys


if options.set == "None":
    options.set = None
config = {"hosts": [(options.host, options.port)]}
if options.alternate:
    config["use_services_alternate"] = True
try:
    client = aerospike.client(config).connect(options.username, options.password)
except ex.ClientError as e:
    print("failed to connect to the cluster with", config["hosts"])
    sys.exit(1)

pp = pprint.PrettyPrinter(indent=2)
key = ("test", "workshop", "world-records")
try:
    client.remove(key)
except:
    pass

wr_100m_1 = [
    [10.06, "Bob Hayes", "Tokyo, Japan", "October 15, 1964"],
    [10.03, "Jim Hines", "Sacramento, USA", "June 20, 1968"],
    [10.02, "Charles Greene", "Mexico City, Mexico", "October 13, 1968"],
    [9.95, "Jim Hines", "Mexico City, Mexico", "October 14, 1968"],
    [9.93, "Calvin Smith", "Colorado Springs, USA", "July 3, 1983"],
    [9.93, "Carl Lewis", "Rome, Italy", "August 30, 1987"],
    [9.92, "Carl Lewis", "Seoul, South Korea", "September 24, 1988"],
]
# intentionally overlap two of the results
wr_100m_2 = [
    [9.93, "Carl Lewis", "Rome, Italy", "August 30, 1987"],
    [9.92, "Carl Lewis", "Seoul, South Korea", "September 24, 1988"],
    [9.90, "Leroy Burrell", "New York, USA", "June 14, 1991"],
    [9.86, "Carl Lewis", "Tokyo, Japan", "August 25, 1991"],
    [9.85, "Leroy Burrell", "Lausanne, Switzerland", "July 6, 1994"],
    [9.84, "Donovan Bailey", "Atlanta, USA", "July 27, 1996"],
    [9.79, "Maurice Greene", "Athens, Greece", "June 16, 1999"],
    [9.77, "Asafa Powell", "Athens, Greece", "June 14, 2005"],
    [9.77, "Asafa Powell", "Gateshead, England", "June 11, 2006"],
    [9.77, "Asafa Powell", "Zurich, Switzerland", "August 18, 2006"],
    [9.74, "Asafa Powell", "Rieti, Italy", "September 9, 2007"],
    [9.72, "Usain Bolt", "New York, USA", "May 31, 2008"],
    [9.69, "Usain Bolt", "Beijing, China", "August 16, 2008"],
    [9.58, "Usain Bolt", "Berlin, Germany", "August 16, 2009"],
]
bins = {"scores": wr_100m_1}

try:
    input("Begin?")
    client.put(key, bins, {"ttl": aerospike.TTL_NEVER_EXPIRE})
    ops = [lh.list_set_order("scores", aerospike.LIST_ORDERED)]
    client.operate(key, ops)
    print("\nAdded the first group of world records, set the list as ordered")
except ex.RecordError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(3)

try:
    input("Continue?")
    list_policy = {
        "list_order": aerospike.LIST_ORDERED,
        "write_flags": (
            aerospike.LIST_WRITE_ADD_UNIQUE
            | aerospike.LIST_WRITE_PARTIAL
            | aerospike.LIST_WRITE_NO_FAIL
        ),
    }
    ops = [lh.list_append_items("scores", wr_100m_2, list_policy)]
    client.operate(key, ops)
    print("\nAdded the second group of world records, set write flags to"
        "maintain unique elements in the list")

    k, m, b = client.get(key)
    print("The merged, ordered, unique scores are")
    pp.pprint(b["scores"])

    input("Continue?")
    print("\nFind the closest two results to 10.0 seconds by relative rank")
    ops = [
        lh.list_get_by_value_rank_range_relative(
            "scores",
            [10.00, aerospike.null()],
            -1,
            aerospike.LIST_RETURN_VALUE,
            2,
            False,
        )
    ]
    k, m, b = client.operate(key, ops)
    print(b["scores"])
    print("\n")
except Exception as e:
    print("Error: {}".format(e))
client.close()
