# -*- coding: utf-8 -*-
from args import options
import aerospike
from aerospike import exception as ex
from aerospike_helpers import cdt_ctx as ctxh
from aerospike_helpers.operations import list_operations as lh
from aerospike_helpers.operations import map_operations as mh
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
key = ("test", "workshop", "maps-over-bins")
try:
    client.remove(key)
except:
    pass
try:
    input("Begin?")
    card1 = {
        "last_six": 511111,
        "expires": 202201,
        "cvv": 111,
        "zip": 95008,
        "default": 1,
    }
    cards_ctx = [ctxh.cdt_ctx_map_key("cards")]
    ops = [
        # create the cards list or gracefully skip to the next operation
        mh.map_put(
            "user", "cards", [],
            {
                "map_write_flags": aerospike.MAP_WRITE_FLAGS_CREATE_ONLY
                                 | aerospike.MAP_WRITE_FLAGS_NO_FAIL,
                "map_order": aerospike.MAP_KEY_ORDERED
            },
        ),
        lh.list_append(
            "user", card1, {"list_order": aerospike.LIST_UNORDERED}, cards_ctx,
        ),
        lh.list_get_by_rank(
            "user", -1, aerospike.LIST_RETURN_VALUE, cards_ctx
        ),
    ]
    k, m, b = client.operate_ordered(
        key, ops, policy={"key": aerospike.POLICY_KEY_SEND}
    )
    print("\nAdded card 1.\nThe default card is {}".format(b[2][1]))

    input("Continue?")
    card2 = {"last_six": 522222, "expires": 202202, "cvv": 222, "zip": 95008}
    ops = [
        lh.list_append("user", card2, ctx=cards_ctx),
        lh.list_get_by_rank("user", -1, aerospike.LIST_RETURN_VALUE, cards_ctx),
        lh.list_size("user", cards_ctx),
    ]
    k, m, b = client.operate_ordered(
        key, ops, policy={"key": aerospike.POLICY_KEY_SEND}
    )
    print("\nAdded card 2.\nThe default card is {}".format(b[1][1]))

    input("Continue?")
    card3 = {"last_six": 533333, "expires": 202303, "cvv": 333, "zip": 95008}
    default_card_ctx = [
        # the card with the default field should have the highest rank
        ctxh.cdt_ctx_map_key("cards"),
        ctxh.cdt_ctx_list_rank(-1)
    ]
    second_card_ctx = [
        # the second card in the list
        ctxh.cdt_ctx_map_key("cards"),
        ctxh.cdt_ctx_list_index(1)
    ]
    # add a new credit card, and set it as the default
    ops = [
        lh.list_append("user", card3, ctx=cards_ctx),
        mh.map_remove_by_key(
            "user", "default", aerospike.MAP_RETURN_NONE, default_card_ctx
        ),
        mh.map_put("user", "default", True, ctx=second_card_ctx),
        lh.list_get_by_rank("user", -1, aerospike.LIST_RETURN_VALUE, cards_ctx),
    ]
    k, m, b = client.operate_ordered(
        key, ops, policy={"key": aerospike.POLICY_KEY_SEND}
    )
    print("\nAdded card 3. Changed card 2 to be the default\nThe default card is {}".format(b[3][1]))

    input("Continue?")
    print("\nDocument structure:")
    k, m, b = client.select(key, ["user"])
    pp.pprint(b)
except Exception as e:
    print("Error: {}".format(e))
client.close()
