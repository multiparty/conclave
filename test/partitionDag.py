import salmon.lang as sal
from salmon.comp import dagonly, mpc
from salmon.utils import *
from salmon.codegen.scotch import ScotchCodeGen
import salmon.partition as part


def testPartition():

    @dagonly
    def protocol():

        colsIn1 = [
            defCol("a", "INTEGER", [1]),
            defCol("b", "INTEGER", [1])
        ]

        colsIn2 = [
            defCol("a", "INTEGER", [2]),
            defCol("b", "INTEGER", [2])
        ]

        in1 = sal.create("in1", colsIn1, set([1]))
        in2 = sal.create("in2", colsIn2, set([2]))

        mult1 = sal.multiply(in1, "mult1", "c", ["a", "b"])
        mult2 = sal.multiply(in2, "mult2", "d", ["a", "b"])

        join1 = sal.join(mult1, mult2, "join1", ["a"], ["a"])
        proj1 = sal.project(join1, "projA", ["b"])

        sal.collect(proj1, 1)

        return set([in1, in2])

    dag = protocol()
    part_dag = part.partDag(dag)

    for job in part_dag:
        print(job[0].name)
        print(job[1])


def test_partition_taxi():

    @mpc(1)
    def protocol():
        colsIn1 = [
            defCol("companyID", "INTEGER", [1]),
            defCol("price", "INTEGER", [1])
        ]
        in1 = sal.create("in1", colsIn1, set([1]))
        colsIn2 = [
            defCol("companyID", "INTEGER", [2]),
            defCol("price", "INTEGER", [2])
        ]
        in2 = sal.create("in2", colsIn2, set([2]))
        colsIn3 = [
            defCol("companyID", "INTEGER", [3]),
            defCol("price", "INTEGER", [3])
        ]
        in3 = sal.create("in3", colsIn3, set([3]))

        cab_data = sal.concat([in1, in2, in3], "cab_data")

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
        return set([in1, in2, in3])

    dag = protocol()
    mapping = part.heupart(dag, ["sharemind"], ["spark"])
    expected = '''sparkcreate->in1,
project->selected_input_0,
aggregation->local_rev_0,
divide->scaled_down_0_0{1}###sparkcreate->in2,
project->selected_input_1,
aggregation->local_rev_1,
divide->scaled_down_0_1{2}###sparkcreate->in3,
project->selected_input_2,
aggregation->local_rev_2,
divide->scaled_down_0_2{3}###sharemindcreatempc->scaled_down_0_0,
closempc->scaled_down_0_0_close,
creatempc->scaled_down_0_1,
closempc->scaled_down_0_1_close,
creatempc->scaled_down_0_2,
closempc->scaled_down_0_2_close,
concatmpc->cab_data,
aggregationmpc->local_rev_obl,
multiplympc->first_val_blank,
multiplympc->local_rev_scaled,
aggregationmpc->total_rev,
joinmpc->local_total_rev,
dividempc->market_share,
multiplympc->market_share_squared,
aggregationmpc->hhi,
openmpc->hhi_open{1, 2, 3}###sparkcreate->hhi_open,
project->hhi_only{1}'''
    actual = "###".join([fmwk + str(subdag) + str(parties) for (fmwk, subdag, parties) in mapping])
    assert expected == actual, actual

if __name__ == "__main__":

    testPartition()
    test_partition_taxi()
