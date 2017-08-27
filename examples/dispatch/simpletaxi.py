from salmon.codegen.sharemind import SharemindCodeGen
import salmon.dispatch
import salmon.net
from salmon.comp import dagonly
import salmon.lang as sal
from salmon.utils import *
from multiprocessing import Process
import sys


@dagonly
def protocol():

    # define inputs
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
    
    cl1 = sal._close(in1, "cl1", set([1, 2, 3]))
    cl2 = sal._close(in2, "cl2", set([1, 2, 3]))
    cl3 = sal._close(in3, "cl3", set([1, 2, 3]))
    cab_data = sal.concat([cl1, cl2, cl3], "cab_data")

    selected_input = sal.project(cab_data, "selected_input", ["companyID", "price"])
    local_rev = sal.aggregate(selected_input, "local_rev", "companyID", "price", "+", "local_rev")
    scaled_down = sal.divide(local_rev, "scaled_down", "local_rev", ["local_rev", 1000])
    first_val_blank = sal.multiply(scaled_down, "first_val_blank", "companyID", ["companyID", 0])
    local_rev_scaled = sal.multiply(first_val_blank, "local_rev_scaled", "local_rev", ["local_rev", 100])
    total_rev = sal.aggregate(first_val_blank, "total_rev", "companyID", "local_rev", "+", "global_rev")
    local_total_rev = sal.join(local_rev_scaled, total_rev, "local_total_rev", "companyID", "companyID")
    market_share = sal.divide(local_total_rev, "market_share", "local_rev", ["local_rev", "global_rev"])
    market_share_squared = sal.multiply(market_share, "market_share_squared", "local_rev",
                                        ["local_rev", "local_rev", 1])
    hhi = sal.aggregate(market_share_squared, "hhi", "local_rev", "companyID", "+", "hhi")

    hhi_opened = sal._open(hhi, "hhi_opened", 1)

    # return root nodes
    return set([in1, in2, in3])

def party_proc(pid):

    sharemind_home = "/home/sharemind/Sharemind-SDK/sharemind/client"
    config = {
        "pid": pid,
        "parties": {
            1: {"host": "localhost", "port": 9001},
            2: {"host": "localhost", "port": 9002},
            3: {"host": "localhost", "port": 9003}
        }
    }
    peer = salmon.net.setup_peer(config)

    job = SharemindCodeGen(protocol(), pid).generate("job-" + str(pid), sharemind_home)
    job_queue = [job]
    salmon.dispatch.dispatch_all(peer, job_queue)

if __name__ == "__main__":

    # run each party in separate process
    # TODO: switch to threads if asyncio is thread-safe
    procs = []
    for pid in [1, 2, 3]:
        p = Process(target=party_proc, args=(pid,))
        p.start()
        procs.append(p)
    # wait for processes to complete
    for p in procs:
        p.join()
