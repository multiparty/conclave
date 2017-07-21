import socket
import sys
import pickle

class DoneMsg():

    def __init__(self, party_id, job_name):

        self.party_id = party_id
        self.job_name = job_name

class Other():

    def __init__(self):

        self.attr = "other"

s = socket.socket(
    socket.AF_INET, socket.SOCK_STREAM)
s.connect(("localhost", 9001))
# msg = sys.argv[1] + "\n"
msg = DoneMsg("1", "job1")
pickled_msg = pickle.dumps(msg)
s.send(pickled_msg + b"\n\n\n")
s.send(pickle.dumps(Other()) + b"\n\n\n")
# f = s.makefile("wb")

# pickle.dump(msg, f, pickle.HIGHEST_PROTOCOL)
# f.close()
# f = s.makefile("wb")
# pickle.dump(msg, f, pickle.HIGHEST_PROTOCOL)
# f.close()
# print(pickled_msg)
# s.send(msg.encode("utf-8"))
