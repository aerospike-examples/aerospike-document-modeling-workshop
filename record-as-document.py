import aerospike
from aerospike import exception as ex
from aerospike_helpers.operations import list_operations as lh
from aerospike_helpers.operations import map_operations as mh
from aerospike_helpers.operations import operations as oh
import pprint
import sys

config = {"hosts": [("192.168.141.136", 31396)], "use_services_alternate": True}
try:
    client = aerospike.client(config).connect()
except ex.ClientError as e:
    print("failed to connect to the cluster with", config["hosts"])
    sys.exit(1)

pp = pprint.PrettyPrinter(indent=2)
key = ("test", "workshop", "record-as-document")
try:
    client.remove(key)
except:
    pass
try:
    input("Begin?")
    print("\nCreate the record with a single bin, integer value")
    client.put(key, {"i": 1}, policy={"key": aerospike.POLICY_KEY_SEND})
    try:
        # Get the key by its digest
        digest = client.get_key_digest("test", "workshop", "record-as-document")
        print("Done.\nThis record's digest is {}".format(digest))
        print("Get the current record by its digest")
        by_digest = ("test", "workshop", None, digest)
        k, m, b = client.get(by_digest, {"key": aerospike.POLICY_KEY_SEND})
        pp.pprint(b)
    except ex.RecordNotFound:
        print("Record not found:", key)

    input("Continue?")
    print("\nAdd a list bin and a map bin to the record")
    example_map = {"b": 2, "d": 4, "💀": ["🦄", {"🌈": "💰"}]}
    ops = [
        oh.write("l", ["🐟", "🌮"]),
        oh.write("m", example_map),
        mh.map_set_policy("m", {"map_order": aerospike.MAP_KEY_ORDERED}),
    ]
    k, m, b = client.operate_ordered(
        key, ops, policy={"key": aerospike.POLICY_KEY_SEND}
    )
    print("Document structure:")
    k, m, b = client.get(key)
    pp.pprint(b)

    input("Continue?")
    print(
        "\nAdd an 🥑 to the list at 'l'; increment the 'i' counter by 3; add"
        "{a: 1, z:26} to the map at 'm'"
    )
    k, m, b = client.operate_ordered(
        key,
        [
            oh.increment("i", 3),
            lh.list_append("l", "🥑"),
            mh.map_put_items("m", {"a": 1, "z": 26}),
        ],
        policy={"key": aerospike.POLICY_KEY_SEND},
    )
    print("Document structure:")
    k, m, b = client.get(key)
    pp.pprint(b)
except Exception as e:
    print("Error: {}".format(e))
client.close()
