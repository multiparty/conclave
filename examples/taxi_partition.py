import salmon.lang as sal
from salmon.comp import mpc, scotch, dagonly
from salmon.utils import *
import salmon.partition as part

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

        selected_input = sal.project(cab_data, "selected_input", ["companyID", "price"])
        local_rev = sal.aggregate(selected_input, "local_rev", ["companyID"], "price", "+", "local_rev")
        scaled_down = sal.divide(local_rev, "scaled_down", "local_rev", ["local_rev", 1000])
        first_val_blank = sal.multiply(scaled_down, "first_val_blank", "companyID", ["companyID", 0])
        local_rev_scaled = sal.multiply(first_val_blank, "local_rev_scaled", "local_rev", ["local_rev", 100])
        total_rev = sal.aggregate(first_val_blank, "total_rev", ["companyID"], "local_rev", "+", "global_rev")
        local_total_rev = sal.join(local_rev_scaled, total_rev, "local_total_rev", ["companyID"], ["companyID"])
        market_share = sal.divide(local_total_rev, "market_share", "local_rev", ["local_rev", "global_rev"])
        market_share_squared = sal.multiply(market_share, "market_share_squared", "local_rev",
                                            ["local_rev", "local_rev", 1])
        hhi = sal.aggregate(market_share_squared, "hhi", ["companyID"], "local_rev", "+", "hhi")
        
        sal.collect(hhi, 1)
        
        # return root nodes
        return set([in1, in2, in3])

    dag = protocol()
    part_dag = part.partDag(dag)

    for job in part_dag:
        print(job[0].name)
        print(job[1])

if __name__ == "__main__":

    test_partition_taxi()
