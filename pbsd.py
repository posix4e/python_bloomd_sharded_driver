from pybloomd import BloomdClient
import random


class BloomRouter(object):
    "Automagically connects and sends keys to the right filter and server"
    def __init__(self, server, filter_prefix="default-", filters=16):
        """
        Creates a new Bloom Router .

        :Parameters:
            - server: Provided as a string, either as "host" or "host:port" or "host:port:udpport".
                      Uses the default port of 8673 if none is provided for tcp, and 8674 for udp.
            - prefix: The prefix to affix to every bloomfilter
            - filters (optional): Number of filters to spread load against. Defaults to 16.
        """
        self.connection = BloomdClient(server)
        self.filters = filters
        self.prefix = filter_prefix
        for i in range(filters):
            self.connection.create_filter("{}-{}".format(filter_prefix, i))

    def get(self, items):
        """
        Multi get all of the correct keys to the correct filters

        :Parameters:
            - items: The set of items to get!
        """
        shard_hash = _get_shard_hash(items, self.filters)
        return all([all(self.connection["{}-{}".format(self.prefix, shard)].multi(items)) 
                    for shard, items_per_shard in shard_hash.iteritems()])

    def add(self, items):
        """
        Bulk add all of the correct keys to the correct filter"

        :Parameters:
            - items: The set of items to get!
        """
        shard_hash = _get_shard_hash(items, self.filters)
        for shard, items_per_shard in shard_hash.iteritems():
            self.connection["{}-{}".format(self.prefix, shard)].bulk(items)


def _get_shard(item, number_of_filters):
    return hash(item) % number_of_filters


def _get_shard_hash(items, number_of_filters):
    item_by_shard = {}

    for item in items:
        shard = _get_shard(item, number_of_filters)
        if shard not in item_by_shard:
            item_by_shard[shard] = []
        item_by_shard[shard].append(item)

    return item_by_shard


client = BloomRouter(["bar"], "Random_prefix{}".format(random.randint(1, 100000)))

keys = ["Test key{} ".format(random.randint(1, 10000))]
# Set a property and check it exists
client.add(keys)
assert client.get(keys) is True

keys = ["Test key{} ".format(0)]
assert client.get(keys) is False
