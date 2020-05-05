import aerospike
from aerospike import exception as ex
from aerospike_helpers.operations import map_operations as mh
import pprint
import sys

config = {"hosts": [("192.168.141.136", 31396)], "use_services_alternate": True}
try:
    client = aerospike.client(config).connect()
except ex.ClientError as e:
    print("failed to connect to the cluster with", config["hosts"])
    sys.exit(1)

pp = pprint.PrettyPrinter(indent=2)
key = ("test", "workshop", "map-terms")
try:
    client.remove(key)
except:
    pass
try:
    example = {"a": 1, "b": 0, "c": 9, "yy": 1, "z": 2}
    pp.pprint(example)
    ops = [
        mh.map_put_items("m", example, {"map_order": aerospike.MAP_KEY_ORDERED}),
        mh.map_get_by_index("m", -1, aerospike.MAP_RETURN_KEY),
        mh.map_get_by_index("m", 0, aerospike.MAP_RETURN_KEY),
        mh.map_get_by_rank("m", -1, aerospike.MAP_RETURN_KEY_VALUE),
        mh.map_get_by_rank("m", 0, aerospike.MAP_RETURN_KEY_VALUE),
        mh.map_get_by_value("m", 1, aerospike.MAP_RETURN_KEY),
    ]
    k, m, b = client.operate_ordered(
        key, ops, policy={"key": aerospike.POLICY_KEY_SEND}
    )
    print(
        (
            "K-Ordered map\n"
            "index -1: {}\n"
            "index 0: {}\n"
            "rank -1: {} ({})\n"
            "rank 0: {} ({})\n"
            "value 1: {}"
        ).format(
            b[1][1],  # access the tuple (bin, val)
            b[2][1],  # for the MAP_RETURN_KEY return type
            b[3][1][0],
            b[3][1][1],  # access the tuple (bin, [map-key, map-value])
            b[4][1][0],  # for the MAP_RETURN_KEY_VALUE return type
            b[4][1][1],
            b[5][1],
        )
    )
except Exception as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
client.close()
