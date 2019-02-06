from multiprocessing.pool import ThreadPool
from unittest import TestCase

from conclave.codegen.libs.python import pub_intersect_as_client, pub_intersect_as_server


class TestPython(TestCase):

    @staticmethod
    def run_both(client_code, server_code):
        pool = ThreadPool(processes=2)
        async_client_actual = pool.apply_async(client_code)
        async_server_actual = pool.apply_async(server_code)
        client_actual = async_client_actual.get()
        server_actual = async_server_actual.get()
        return {"client": client_actual, "server": server_actual}

    def test_intersect(self):
        def client():
            in_rel = [[2, 1], [1, 2], [3, 3]]
            return pub_intersect_as_client("localhost", 8001, in_rel, 0)

        def server():
            in_rel = [[2, 1], [2, 2], [1, 3], [5, 4]]
            return pub_intersect_as_server("localhost", 8001, in_rel, 0)

        actuals = self.run_both(client, server)
        self.assertEqual(actuals["client"], actuals["server"])
        expected = [[1], [2]]
        actual = sorted(actuals["client"])
        self.assertEqual(expected, actual, "Expected {} but was {}".format(expected, actual))
