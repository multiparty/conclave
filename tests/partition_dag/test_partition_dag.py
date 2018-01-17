import unittest
from unittest import TestCase
import warnings
import salmon.lang as sal
import salmon.dag as saldag
from salmon.comp import dag_only, mpc
from salmon.utils import *
from salmon.codegen.scotch import ScotchCodeGen
import salmon.partition as part
import os


# suppresses annoying warnings about open files
def ignore_resource_warnings(test_func):

    def do_test(self, *args, **kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ResourceWarning)
            test_func(self, *args, **kwargs)

    return do_test


class TestConclave(TestCase):

    @ignore_resource_warnings
    def check_workflow(self, dag, name):

        mapping = part.heupart(dag, ["sharemind"], ["spark"])
        actual = "###".join([fmwk + str(subdag) + str(parties)
                             for (fmwk, subdag, parties) in mapping])

        expected_rootdir = "{}/part_expected".format(os.path.dirname(os.path.realpath(__file__)))

        # uncomment this to regenerate (needed if rewrite logic changes)
        # open(expected_rootdir + '/{}'.format(name), 'w').write(actual)

        expected = open(expected_rootdir + '/{}'.format(name), 'r').read()
        self.assertEqual(expected, actual)

    def test_partition_taxi(self):

        @mpc(1)
        def protocol():
            cols_in_1 = [
                defCol("companyID", "INTEGER", [1]),
                defCol("price", "INTEGER", [1])
            ]
            in_1 = sal.create("yellow1", cols_in_1, set([1]))
            cols_in_2 = [
                defCol("companyID", "INTEGER", [2]),
                defCol("price", "INTEGER", [2])
            ]
            in_2 = sal.create("yellow2", cols_in_2, set([2]))
            cols_in_3 = [
                defCol("companyID", "INTEGER", [3]),
                defCol("price", "INTEGER", [3])
            ]
            in_3 = sal.create("yellow3", cols_in_3, set([3]))

            cab_data = sal.concat([in_1, in_2, in_3], "cab_data")

            selected_input = sal.project(
                cab_data, "selected_input", ["companyID", "price"])
            local_rev = sal.aggregate(selected_input, "local_rev", [
                "companyID"], "price", "+", "local_rev")
            scaled_down = sal.divide(
                local_rev, "scaled_down", "local_rev", ["local_rev", 1000])
            first_val_blank = sal.multiply(
                scaled_down, "first_val_blank", "companyID", ["companyID", 0])
            local_rev_scaled = sal.multiply(
                first_val_blank, "local_rev_scaled", "local_rev", ["local_rev", 100])
            total_rev = sal.aggregate(first_val_blank, "total_rev", [
                "companyID"], "local_rev", "+", "global_rev")
            local_total_rev = sal.join(local_rev_scaled, total_rev, "local_total_rev", [
                "companyID"], ["companyID"])
            market_share = sal.divide(local_total_rev, "market_share", "local_rev", [
                "local_rev", "global_rev"])
            market_share_squared = sal.multiply(market_share, "market_share_squared", "local_rev",
                                                ["local_rev", "local_rev", 1])
            hhi = sal.aggregate(market_share_squared, "hhi", [
                "companyID"], "local_rev", "+", "hhi")
            # dummy projection to force non-mpc subdag
            hhi_only = sal.project(
                hhi, "hhi_only", ["companyID", "hhi"])

            sal.collect(hhi_only, 1)

            # return root nodes
            return set([in_1, in_2, in_3])

        dag = protocol()
        self.check_workflow(dag, 'taxi')

    def test_partition_ssn(self):

        def hybrid_join():

            # define inputs
            cols_in_a = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1]),
            ]
            in_1 = sal.create("govreg", cols_in_a, set([1]))
            in_1.isMPC = False

            proj_a = sal.project(in_1, "proj_a", ["a", "b"])
            proj_a.isMPC = False
            proj_a.out_rel.storedWith = set([1])

            cols_in_b = [
                defCol("c", "INTEGER", [1], [2]),
                defCol("d", "INTEGER", [2])
            ]
            in_2 = sal.create("company0", cols_in_b, set([2]))
            in_2.isMPC = False

            proj_b = sal.project(in_2, "proj_b", ["c", "d"])
            proj_b.isMPC = False
            proj_b.out_rel.storedWith = set([2])

            cols_in_c = [
                defCol("c", "INTEGER", [1], [3]),
                defCol("d", "INTEGER", [3])
            ]
            in_3 = sal.create("company1", cols_in_c, set([3]))
            in_3.isMPC = False

            proj_c = sal.project(in_3, "proj_c", ["c", "d"])
            proj_c.isMPC = False
            proj_c.out_rel.storedWith = set([3])

            cl_a = sal._close(proj_a, "cl_a", set([1, 2, 3]))
            cl_a.isMPC = True
            cl_b = sal._close(proj_b, "cl_b", set([1, 2, 3]))
            cl_b.isMPC = True
            cl_c = sal._close(proj_c, "cl_c", set([1, 2, 3]))
            cl_c.isMPC = True

            right_closed = sal.concat([cl_b, cl_c], "clD")
            right_closed.isMPC = True
            right_closed.out_rel.storedWith = set([1, 2, 3])

            shuffled_a = sal.shuffle(cl_a, "shuffled_a")
            shuffled_a.isMPC = True
            persisted_a = sal._persist(shuffled_a, "persisted_a")
            persisted_a.isMPC = True
            shuffled_b = sal.shuffle(right_closed, "shuffled_b")
            shuffled_b.isMPC = True
            persisted_b = sal._persist(shuffled_b, "persisted_b")
            persisted_b.isMPC = True

            keys_a_closed = sal.project(shuffled_a, "keys_a_closed", ["a"])
            keys_a_closed.out_rel.storedWith = set([1, 2, 3])
            keys_a_closed.isMPC = True
            keys_b_closed = sal.project(shuffled_b, "keys_b_closed", ["c"])
            keys_b_closed.isMPC = True
            keys_b_closed.out_rel.storedWith = set([1, 2, 3])

            keys_a = sal._open(keys_a_closed, "keys_a", 1)
            keys_a.isMPC = True
            keys_b = sal._open(keys_b_closed, "keys_b", 1)
            keys_b.isMPC = True

            indexed_a = sal.index(keys_a, "indexed_a", "index_a")
            indexed_a.isMPC = False
            indexed_a.out_rel.storedWith = set([1])
            indexed_b = sal.index(keys_b, "indexed_b", "index_b")
            indexed_b.isMPC = False
            indexed_b.out_rel.storedWith = set([1])

            joined_indeces = sal.join(
                indexed_a, indexed_b, "joined_indeces", ["a"], ["c"])
            joined_indeces.isMPC = False
            joined_indeces.out_rel.storedWith = set([1])

            indeces_only = sal.project(
                joined_indeces, "indeces_only", ["index_a", "index_b"])
            indeces_only.isMPC = False
            indeces_only.out_rel.storedWith = set([1])

            indeces_closed = sal._close(
                indeces_only, "indeces_closed", set([1, 2, 3]))
            indeces_closed.isMPC = True

            joined = sal._index_join(persisted_a, persisted_b, "joined", [
                "a"], ["c"], indeces_closed)
            joined.isMPC = True

            return joined, set([in_1, in_2, in_3])

        def hybrid_agg(in1):

            shuffled = sal.shuffle(in1, "shuffled")
            shuffled.out_rel.storedWith = set([1, 2, 3])
            shuffled.isMPC = True

            persisted = sal._persist(shuffled, "persisted")
            persisted.out_rel.storedWith = set([1, 2, 3])
            persisted.isMPC = True

            keys_closed = sal.project(shuffled, "keys_closed", ["b"])
            keys_closed.out_rel.storedWith = set([1, 2, 3])
            keys_closed.isMPC = True

            keys = sal._open(keys_closed, "keys", 1)
            keys.isMPC = True

            indexed = sal.index(keys, "indexed", "rowIndex")
            indexed.isMPC = False
            indexed.out_rel.storedWith = set([1])

            sorted_by_key = sal.sort_by(indexed, "sorted_by_key", "b")
            sorted_by_key.isMPC = False
            sorted_by_key.out_rel.storedWith = set([1])

            eq_flags = sal._comp_neighs(sorted_by_key, "eq_flags", "b")
            eq_flags.isMPC = False
            eq_flags.out_rel.storedWith = set([1])

            # TODO: should be a persist op
            sorted_by_key_stored = sal.project(
                sorted_by_key, "sorted_by_key_stored", ["rowIndex", "b"])
            sorted_by_key_stored.isMPC = False
            sorted_by_key_stored.out_rel.storedWith = set([1])

            closed_eq_flags = sal._close(eq_flags, "closed_eq_flags", set([1, 2, 3]))
            closed_eq_flags.isMPC = True
            closed_sorted_by_key = sal._close(
                sorted_by_key_stored, "closed_sorted_by_key", set([1, 2, 3]))
            closed_sorted_by_key.isMPC = True

            agg = sal.index_aggregate(
                persisted, "agg", ["b"], "d", "+", "d", closed_eq_flags, closed_sorted_by_key)
            agg.isMPC = True
            sal._open(agg, "ssnopened", 1)

        def protocol():

            joined_res, inputs = hybrid_join()
            hybrid_agg(joined_res)

            return saldag.OpDag(inputs)

        dag = protocol()
        self.check_workflow(dag, 'ssn')

    def test_inputs_out_of_order(self):

        def protocol():

            # define inputs
            cols_in_a = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1]),
            ]
            in_1 = sal.create("in_1", cols_in_a, set([1]))
            in_1.isMPC = False

            proj_a = sal.project(in_1, "proj_a", ["a", "b"])
            proj_a.isMPC = False
            proj_a.out_rel.storedWith = set([1])

            cols_in_b = [
                defCol("c", "INTEGER", [1], [2]),
                defCol("d", "INTEGER", [2])
            ]
            in_2 = sal.create("in_2", cols_in_b, set([2]))
            in_2.isMPC = False

            proj_b = sal.project(in_2, "proj_b", ["c", "d"])
            proj_b.isMPC = False
            proj_b.out_rel.storedWith = set([2])

            cols_in_c = [
                defCol("c", "INTEGER", [1], [3]),
                defCol("d", "INTEGER", [3])
            ]
            in_3 = sal.create("beforeOthers", cols_in_c, set([1, 2, 3]))
            in_3.isMPC = True

            cl_a = sal._close(proj_a, "cl_a", set([1, 2, 3]))
            cl_a.isMPC = True
            cl_b = sal._close(proj_b, "cl_b", set([1, 2, 3]))
            cl_b.isMPC = True
            cl_c = sal._close(in_3, "cl_c", set([1, 2, 3]))
            cl_c.isMPC = True

            right_closed = sal.concat([cl_a, cl_b, cl_c], "a")
            right_closed.isMPC = True
            right_closed.out_rel.storedWith = set([1, 2, 3])

            shuffled_a = sal.shuffle(cl_a, "shuffled_a")
            shuffled_a.isMPC = True
            sal._open(shuffled_a, "ssn_opened", 1)

            return saldag.OpDag(set([in_1, in_2, in_3]))

        dag = protocol()
        self.check_workflow(dag, 'out_of_order')

    def test_partition_hybrid_join(self):

        @mpc(1)
        def protocol():

            cols_in_a = [
                defCol("a", "INTEGER", [1]),
                defCol("b", "INTEGER", [1]),
            ]
            in_a = sal.create("in_a", cols_in_a, set([1]))
            proj_a = sal.project(in_a, "proj_a", ["a", "b"])

            cols_in_b = [
                defCol("c", "INTEGER", [1], [2]),
                defCol("d", "INTEGER", [2])
            ]
            in_b = sal.create("in_b", cols_in_b, set([2]))
            proj_b = sal.project(in_b, "proj_b", ["c", "d"])

            joined = sal.join(proj_a, proj_b, "joined", ["a"], ["c"])
            sal.collect(joined, 1)

            return set([in_a, in_b])

        dag = protocol()
        self.check_workflow(dag, 'hybrid_join')


if __name__ == '__main__':
    unittest.main()