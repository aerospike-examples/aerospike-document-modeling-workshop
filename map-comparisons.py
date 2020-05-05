import aerospike
from aerospike import exception as ex
from aerospike_helpers.operations import map_operations as mh
from aerospike_helpers.operations import list_operations as lh
import pprint
import sys

config = {"hosts": [("192.168.141.136", 31396)], "use_services_alternate": True}
try:
    client = aerospike.client(config).connect()
except ex.ClientError as e:
    print("failed to connect to the cluster with", config["hosts"])
    sys.exit(1)

pp = pprint.PrettyPrinter(indent=2)
key = ("test", "workshop", "map-comparisons")
try:
    client.remove(key)
except:
    pass
try:
    ex0 = {"z": 26}
    ex1 = {"a": 1, "b": 2}
    ex2 = {"e": 5, "a":1, "b":2, "c":3}
    ex3 = {"c": 3, "b": 2}
    ex4 = {"b": 2, "c": 3}
    ex5 = {"a": 1}
    examples = [ex0, ex1, ex2, ex3, ex4, ex5]
    ops = [
        lh.list_append_items("l", examples),
        lh.list_get_by_rank("l", 0, aerospike.LIST_RETURN_INDEX),
        lh.list_get_by_rank("l", 1, aerospike.LIST_RETURN_INDEX),
        lh.list_get_by_rank("l", 2, aerospike.LIST_RETURN_INDEX),
        lh.list_get_by_rank("l", 3, aerospike.LIST_RETURN_INDEX),
        lh.list_get_by_rank("l", 4, aerospike.LIST_RETURN_INDEX),
        lh.list_get_by_rank("l", 5, aerospike.LIST_RETURN_INDEX),
    ]
    k, m, b = client.operate_ordered(
        key, ops, policy={"key": aerospike.POLICY_KEY_SEND}
    )
    print(
        (
            "Map elements inserted into the list: {}\n"
            "rank 0 (lowest): {}\n"
            "rank 1: {}\n"
            "rank 2: {}\n"
            "rank 3: {}\n"
            "rank 4: {}\n"
            "rank 5 (highest): {}"
        ).format(
            examples,
            examples[b[1][1]],
            examples[b[2][1]],
            examples[b[3][1]],
            examples[b[4][1]],
            examples[b[5][1]],
            examples[b[6][1]],
        )
    )
except Exception as e:
    print("Error: {}".format(e))
client.close()
