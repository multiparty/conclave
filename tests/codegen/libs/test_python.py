from multiprocessing.pool import ThreadPool
from unittest import TestCase

import conclave.codegen.libs.python as pylib


class TestPython(TestCase):

    @staticmethod
    def run_both(client_code, server_code):
        pool = ThreadPool(processes=2)
        async_client_actual = pool.apply_async(client_code)
        async_server_actual = pool.apply_async(server_code)
        client_actual = async_client_actual.get()
        server_actual = async_server_actual.get()
        return {"client": client_actual, "server": server_actual}

    def test_pub_intersect(self):
        def client():
            in_rel = [[2, 1], [1, 2], [3, 3]]
            return pylib.pub_intersect_as_client("localhost", 8001, in_rel, 0)

        def server():
            in_rel = [[2, 1], [2, 2], [1, 3], [5, 4]]
            return pylib.pub_intersect_as_server("localhost", 8001, in_rel, 0)

        actuals = self.run_both(client, server)
        self.assertEqual(actuals["client"], actuals["server"])
        expected = [[1], [2]]
        actual = sorted(actuals["client"])
        self.assertEqual(expected, actual, "Expected {} but was {}".format(expected, actual))

    def test_pub_join(self):
        def server():
            left = [[2, 5], [3, 6], [1, 7], [5, 8]]
            right = [[5, 9], [6, 10], [5, 11]]
            return pylib.public_join_as_server_part("localhost", 8001, left, right,
                                                    left_key_col=0, right_key_col=0,
                                                    num_left_cols=2, num_right_cols=2)

        def client():
            left = [[2, 1], [2, 2], [1, 3], [3, 4]]
            right = [[2, 4]]
            return pylib.public_join_as_client_part("localhost", 8001, left, right,
                                                    left_key_col=0, right_key_col=0,
                                                    num_left_cols=2, num_right_cols=2)

        actuals = self.run_both(client, server)
        server_expected = [[1, 5, 1], [1, 1, 1], [1, 1, 1], [5, 8, 9], [5, 8, 11]]
        self.assertEqual(server_expected, actuals["server"])
        client_expected = [[2, 1, 4], [2, 1, 4], [2, 2, 4], [1, 1, 1], [1, 1, 1]]
        self.assertEqual(client_expected, actuals["client"])

    def test_pub_join_empty_rels(self):
        def server():
            left = []
            right = [[2, 9], [6, 10], [5, 11]]
            return pylib.public_join_as_server_part("localhost", 8001, left, right,
                                                    left_key_col=0, right_key_col=0,
                                                    num_left_cols=2, num_right_cols=2)

        def client():
            left = [[2, 1], [2, 2], [1, 3], [3, 4]]
            right = []
            return pylib.public_join_as_client_part("localhost", 8001, left, right,
                                                    left_key_col=0, right_key_col=0,
                                                    num_left_cols=2, num_right_cols=2)

        actuals = self.run_both(client, server)
        server_expected = [[2, 1, 9], [2, 1, 9]]
        self.assertEqual(server_expected, actuals["server"])
        client_expected = [[1, 1, 1], [1, 2, 1]]
        self.assertEqual(client_expected, actuals["client"])
