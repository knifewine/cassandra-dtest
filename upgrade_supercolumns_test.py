from dtest import Tester, debug
from ccmlib.cluster import Cluster
from ccmlib.common import get_version_from_build
import random, os, time, re

# Tests upgrade between 1.2->2.0 for super columns (since that's where
# we removed then internally)
class TestSCUpgrade(Tester):

    def __init__(self, *args, **kwargs):
        self.ignore_log_patterns = [
            # This one occurs if we do a non-rolling upgrade, the node
            # it's trying to send the migration to hasn't started yet,
            # and when it does, it gets replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        ]
        Tester.__init__(self, *args, **kwargs)

    def upgrade_with_index_creation_test(self):
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_install_dir(version="1.2.16")
        cluster.populate(2).start()

        [node1, node2] = cluster.nodelist()

        cli = node1.cli()
        cli.do("create keyspace test with placement_strategy = 'SimpleStrategy' and strategy_options = {replication_factor : 2} and durable_writes = true")
        cli.do("use test")
        cli.do("create column family sc_test with column_type = 'Super' and comparator = 'UTF8Type' and subcomparator = 'UTF8Type' and default_validation_class = 'UTF8Type' and key_validation_class = 'UTF8Type'")

        for i in range(0, 2):
            for j in range(0, 2):
                cli.do("set sc_test['k0']['sc%d']['c%d'] = 'v'" % (i, j))

        assert not cli.has_errors(), cli.errors()
        cli.close()

        # Upgrade node 1
        node1.flush()
        time.sleep(.5)
        node1.stop(wait_other_notice=True)
        self.set_node_to_current_version(node1)
        node1.start(wait_other_notice=True)
        time.sleep(.5)

        cli = node1.cli()
        cli.do("use test")
        cli.do("consistencylevel as quorum")

        # Check we can still get data properly
        cli.do("get sc_test['k0']")
        assert_scs(cli, ['sc0', 'sc1'])
        assert_columns(cli, ['c0', 'c1'])

        cli.do("get sc_test['k0']['sc1']")
        assert_columns(cli, ['c0', 'c1'])

        cli.do("get sc_test['k0']['sc1']['c1']")
        assert_columns(cli, ['c1'])

    #CASSANDRA-7188
    def upgrade_with_counters_test(self):
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_install_dir(version="1.2.19")
        cluster.populate(3).start()

        node1, node2, node3 = cluster.nodelist()

        cli = node1.cli()
        cli.do("create keyspace test with placement_strategy = 'SimpleStrategy' and strategy_options = {replication_factor : 2} and durable_writes = true")
        cli.do("use test")
        cli.do("create column family sc_test with column_type = 'Super' and default_validation_class = 'CounterColumnType' AND key_validation_class=UTF8Type AND comparator=UTF8Type")

        for i in xrange(2):
            for j in xrange(2):
                for k in xrange(20):
                    cli.do("incr sc_test['Counter1']['sc%d']['c%d'] by 1" % (i, j))

        assert not cli.has_errors(), cli.errors()
        cli.close()

        node1.drain()
        node1.watch_log_for("DRAINED")
        node1.stop(wait_other_notice=False)
        self.set_node_to_current_version(node1)
        node1.start(wait_other_notice=True)

        cli = node1.cli()
        cli.do("use test")
        for i in xrange(2):
            for j in xrange(2):
                for k in xrange(50):
                    cli.do("incr sc_test['Counter1']['sc%d']['c%d'] by 1" % (i, j))

        cli2 = node2.cli()
        cli2.do("use test")
        for i in xrange(2):
            for j in xrange(2):
                for k in xrange(50):
                    cli2.do("incr sc_test['Counter1']['sc%d']['c%d'] by 1" % (i, j))

        cli3 = node3.cli()
        cli3.do("use test")
        for i in xrange(2):
            for j in xrange(2):
                for k in xrange(50):
                    cli3.do("incr sc_test['Counter1']['sc%d']['c%d'] by 1" % (i, j))

        node2.drain()
        node3.drain()
        node2.watch_log_for("DRAINED")
        node3.watch_log_for("DRAINED")
        node2.stop(wait_other_notice=False)
        node3.stop(wait_other_notice=False)
        self.set_node_to_current_version(node2)
        self.set_node_to_current_version(node3)
        node2.start(wait_other_notice=True)
        node3.start(wait_other_notice=True)

        cli = node1.cli()
        cli.do("use test")
        cli.do("consistencylevel as quorum")

        # Check we can still get data properly
        cli.do("get sc_test['Counter1']")
        assert_scs(cli, ['sc0', 'sc1'])
        assert_counter_columns(cli, ['c0', 'c1'])

        cli.do("get sc_test['Counter1']['sc1']")
        assert_counter_columns(cli, ['c0', 'c1'])

        cli.do("get sc_test['Counter1']['sc1']['c1']")
        assert_counter_columns(cli, ['c1'])

        assert not cli.has_errors(), cli.errors()
        cli.close()

def assert_scs(cli, names):
    assert not cli.has_errors(), cli.errors()
    output = cli.last_output()

    for name in names:
        assert re.search('super_column=%s' % name, output) is not None, 'Cannot find super column %s in %s' % (name, output)

def assert_columns(cli, names):
    assert not cli.has_errors(), cli.errors()
    output = cli.last_output()

    for name in names:
        assert re.search('name=%s' % name, output) is not None, 'Cannot find column %s in %s' % (name, output)

def assert_counter_columns(cli, names):
    assert not cli.has_errors(), cli.errors()
    output = cli.last_output()

    for name in names:
        assert re.search('counter=%s' % name, output) is not None, 'Cannot find column %s in %s' % (name, output)
