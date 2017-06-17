from pybloomd import BloomdClient
import random
import time
import uuid
from nyanbar import NyanBar


class BloomRouter(object):
    "Automagically connects and sends keys to the right filter and server"
    def __init__(self,
                 server,
                 prefix="default-",
                 filter_count=1,
                 # These options we abstract over
                 capacity=16000 * 1000, prob=None,
                 in_memory=False):
        """
        Creates a new Bloom Router .

        :Parameters:
            - server: Provided as a string, either as "host" or "host:port" or "host:port:udpport".
                      Uses the default port of 8673 if none is provided for tcp, and 8674 for udp.
            - prefix: The prefix to affix to every bloomfilter
            - filter_count (optional): The number of filters
            - capacity (optional): Number of elements
            - prob (optional): The probability of errors across that probability
            - in_memory (optional): Should the indexes be in memory

        """
        self.connection = BloomdClient(server)
        self.capacity = capacity
        self.prob = prob
        self.in_memory = in_memory
        self.prefix = prefix
        self.filter_count = filter_count
        # The maximum sized blooms we want to support
        max_capacity = 4000 * 1000 * 1000

        if filter_count * max_capacity < capacity:
            raise Exception("""You want to much memory out of
                            a bloomd filter, we restrict
        to {} per bloomd server. Use more filters"""
                            .format(max_capacity))
        for i in range(filter_count):
            self.connection.create_filter("{}-{}".format(prefix, i),
                                          capacity=capacity / filter_count,
                                          prob=prob,
                                          in_memory=in_memory)

    def get(self, items):
        """
        Multi get all of the correct keys return true if anything is true

        :Parameters:
            - items: The set of items to get!
        """
        shard_hash = _get_shard_hash(items, self.filter_count)
        for shard, items_per_shard in shard_hash.iteritems():
            if any(self.connection["{}-{}".format(self.prefix, shard)].multi(items)):
                return True
        return False

    def all(self, items):
        """
        Multi get all of the correct keys and return true if all of them are true

        :Parameters:
            - items: The set of items to get!
        """
        shard_hash = _get_shard_hash(items, self.filter_count)
        return all([all(self.connection["{}-{}".format(self.prefix, shard)].multi(items))
                    for shard, items_per_shard in shard_hash.iteritems()])

    def raw(self, items):
        """
        Multi get all of the correct keys and return them by filter

        :Parameters:
            - items: The set of items to get!
        """
        return [self.connection["{}-{}".format(self.prefix, shard)].multi(items)
                for shard, items_per_shard in _get_shard_hash(items, self.filter_count).iteritems()]

    def add(self, items):
        """
        Bulk add all of the correct keys to the correct filter"

        :Parameters:
            - items: The set of items to get!
        """
        for shard, items_per_shard in _get_shard_hash(items, self.filter_count).iteritems():
            self.connection["{}-{}".format(self.prefix, shard)].bulk(items)


def _get_shard(item, number_of_filters):
    return hash(item) % number_of_filters


def _get_shard_hash(items, number_of_filters):
    items_by_shard = {}

    for item in items:
        shard = _get_shard(item, number_of_filters)
        if shard not in items_by_shard:
            items_by_shard[shard] = []
        items_by_shard[shard].append(item)

    return items_by_shard
#
# It's al testing code below this
#


def timing(f):
    def wrap(*args):
        time1 = time.time()
        ret = f(*args)
        time2 = time.time()
        print '%s function took %0.3f ms' % (f.func_name, (time2-time1)*1000.0)
        return ret
    return wrap


num_keys = 8192
testsize = 10


@timing
def test_one_node():
    hosts = ["bloom1"]
    client = BloomRouter(hosts, "x{}".format(random.randint(1, 100000)))
    keys = [str(uuid.uuid4()) for _ in range(num_keys)]
    client.add(keys)

    assert client.get(keys)
    assert client.all(keys)


@timing
def test_many_nodes():
    hosts = ["bloom1", "bloom2", "bloom3", "bloom4"]
    client = BloomRouter(hosts, "h{}".format(random.randint(1, 100000)))
    keys = [str(uuid.uuid4()) for _ in range(num_keys)]
    client.add(keys)

    assert client.get(keys)
    assert client.all(keys)


@timing
def benchmark_put():
    hosts = ["bloom1", "bloom2", "bloom3", "bloom4"]
    client = BloomRouter(hosts, "g{}".format(random.randint(1, 100000)))
    progress = NyanBar(tasks=testsize)
    for i in range(testsize):
        progress.task_done()
        keys = [str(uuid.uuid4()) for _ in range(num_keys)]
        client.add(keys)
    progress.finish()


@timing
def benchmark_put_with_many_filters():
    hosts = ["bloom1", "bloom2", "bloom3", "bloom4"]
    client = BloomRouter(hosts, "g{}".format(random.randint(1, 100000), filter_count=16))
    progress = NyanBar(tasks=testsize)
    for i in range(testsize):
        progress.task_done()
        keys = [str(uuid.uuid4()) for _ in range(num_keys)]
        client.add(keys)
    progress.finish()


@timing
def put_then_get_with_one_filter():
    hosts = ["bloom1", "bloom2", "bloom3", "bloom4"]
    client = BloomRouter(hosts, "f{}".format(random.randint(1, 100000)), filter_count=1)
    progress = NyanBar(tasks=testsize)
    for i in range(testsize):
        progress.task_done()
        keys = [str(uuid.uuid4()) for _ in range(num_keys)]
        client.add(keys)
        assert client.get(keys)
    progress.finish()


@timing
def put_then_get_with_many_filters():
    hosts = ["bloom1", "bloom2", "bloom3", "bloom4"]
    client = BloomRouter(hosts, "f{}".format(random.randint(1, 100000)), filter_count=16)
    progress = NyanBar(tasks=testsize)
    for i in range(testsize):
        progress.task_done()
        keys = [str(uuid.uuid4()) for _ in range(num_keys)]
        client.add(keys)
        assert client.get(keys)
    progress.finish()


print("TESTS Go!")
test_one_node()
test_many_nodes()
print("TESTS DONE!")
print("Benchmarks Go!")
benchmark_put()
benchmark_put_with_many_filters()
put_then_get_with_one_filter()
put_then_get_with_many_filters()
print("Benchmarks DONE!")
