import random, re, time, uuid

from dtest import Tester, debug
from pyassertions import assert_invalid
from cassandra import InvalidRequest
from cassandra.query import BatchStatement, SimpleStatement
from cassandra.protocol import ConfigurationException


class TestSecondaryIndexes(Tester):

    def bug3367_test(self):
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)

        columns = {"password": "varchar", "gender": "varchar", "session_token": "varchar", "state": "varchar", "birth_year": "bigint"}
        self.create_cf(cursor, 'users', columns=columns)

        # insert data
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user2', 'ch@ngem3b', 'm', 'CA', 1971);")

        # create index
        cursor.execute("CREATE INDEX gender_key ON users (gender);")
        cursor.execute("CREATE INDEX state_key ON users (state);")
        cursor.execute("CREATE INDEX birth_year_key ON users (birth_year);")

        # insert data
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
        cursor.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user4', 'ch@ngem3d', 'm', 'TX', 1974);")

        result = cursor.execute("SELECT * FROM users;")
        assert len(result) == 4, "Expecting 4 users, got" + str(result)

        result = cursor.execute("SELECT * FROM users WHERE state='TX';")
        assert len(result) == 2, "Expecting 2 users, got" + str(result)

        result = cursor.execute("SELECT * FROM users WHERE state='CA';")
        assert len(result) == 1, "Expecting 1 users, got" + str(result)

    def test_8280_validate_indexed_values(self):
        """Tests CASSANDRA-8280

        Reject inserts & updates where values of any indexed
        column is > 64k
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        conn = self.patient_cql_connection(node1)
        cursor = conn
        self.create_ks(cursor, 'ks', 1)

        self.insert_row_with_oversize_value("CREATE TABLE %s(a int, b int, c text, PRIMARY KEY (a))",
                                            "CREATE INDEX ON %s(c)",
                                            "INSERT INTO %s (a, b, c) VALUES (0, 0, ?)",
                                            cursor)

        self.insert_row_with_oversize_value("CREATE TABLE %s(a int, b text, c int, PRIMARY KEY (a, b))",
                                            "CREATE INDEX ON %s(b)",
                                            "INSERT INTO %s (a, b, c) VALUES (0, ?, 0)",
                                            cursor)

        self.insert_row_with_oversize_value("CREATE TABLE %s(a text, b int, c int, PRIMARY KEY ((a, b)))",
                                            "CREATE INDEX ON %s(a)",
                                            "INSERT INTO %s (a, b, c) VALUES (?, 0, 0)",
                                            cursor)

        self.insert_row_with_oversize_value("CREATE TABLE %s(a int, b text, PRIMARY KEY (a)) WITH COMPACT STORAGE",
                                            "CREATE INDEX ON %s(b)",
                                            "INSERT INTO %s (a, b) VALUES (0, ?)",
                                            cursor)

    def insert_row_with_oversize_value(self, create_table_cql, create_index_cql, insert_cql, cursor):
        """ Validate two variations of the supplied insert statement, first
        as it is and then again transformed into a conditional statement
        """
        table_name = "table_" + str(int(round(time.time() * 1000)))
        cursor.execute(create_table_cql % table_name)
        cursor.execute(create_index_cql % table_name)
        value = "X" * 65536
        self._assert_invalid_request(cursor, insert_cql % table_name, value)
        self._assert_invalid_request(cursor, (insert_cql % table_name) + ' IF NOT EXISTS', value)

    def _assert_invalid_request(self, cursor, insert_cql, value):
        """ Perform two executions of the supplied statement, as a
        single statement and again as part of a batch
        """
        prepared = cursor.prepare(insert_cql)
        self._execute_and_fail(lambda: cursor.execute(prepared, [value]), insert_cql)
        batch = BatchStatement()
        batch.add(prepared, [value])
        self._execute_and_fail(lambda: cursor.execute(batch), insert_cql)

    def _execute_and_fail(self, operation, cql_string):
        try:
            operation()
            assert False, "Expecting query %s to be invalid" % cql_string
        except AssertionError as e:
            raise e
        except InvalidRequest:
            pass

    def wait_for_schema_agreement(self, cursor):
        rows = cursor.execute("SELECT schema_version FROM system.local")
        local_version = rows[0]

        all_match = True
        rows = cursor.execute("SELECT schema_version FROM system.peers")
        for peer_version in rows:
            if peer_version != local_version:
                all_match = False
                break

        if all_match:
            return
        else:
            time.sleep(0.10)
            self.wait_for_schema_agreement(cursor)
