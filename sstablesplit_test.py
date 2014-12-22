from dtest import Tester, debug

from os.path import getsize
import time

class TestSSTableSplit(Tester):

    def split_test(self):
        """
        Check that after running compaction, sstablessplit can succesfully split
        The resultant sstable.  Check that split is reversable and that data is readable
        after carrying out these operations.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        # Here we need to wait to connect
        # to the node. This is required for windows
        # to prevent stress starting before the node
        # is ready for connections
        node.watch_log_for('thrift clients...')
        debug("Run stress to insert data")
        node.stress( ['-o', 'insert'] )

        self._do_compaction(node)
        self._do_split(node, version)
        self._do_compaction(node)
        self._do_split(node, version)

        debug("Run stress to ensure data is readable")
        node.stress( ['-o', 'read'] )

    def _do_compaction(self, node):
        debug("Compact sstables.")
        node.flush()
        node.compact()
        node.flush()
        keyspace = 'Keyspace1'
        sstables = node.get_sstables(keyspace, '')
        debug("Number of sstables after compaction: %s" % len(sstables))

    def _do_split(self, node, version):
        debug("Run sstablesplit")
        time.sleep(5.0)
        node.stop()
        keyspace = 'Keyspace1'
        origsstable = node.get_sstables(keyspace, '')
        debug("Original sstable before split: %s" % origsstable)
        node.run_sstablesplit( keyspace=keyspace )
        sstables = node.get_sstables(keyspace, '')
        debug("Number of sstables after split: %s" % len(sstables))
        assert len(sstables) == 6, "Incorrect number of sstables after running sstablesplit."
        assert max( [ getsize( sstable ) for sstable in sstables ] ) <= 52428960, "Max sstables size should be 52428960."
        node.start()
