# Conclave demo

Begin by downloading and following install instructions for the JIFF library: https://github.com/multiparty/jiff
Next, you should use a virtualenv running Python3.5: https://virtualenv.pypa.io/en/latest/installation.html

Conclave can be run by submitting a protocol like the following:

### protocol.py
```python
import sys
import json
from conclave.lang import *
from conclave.utils import *
from conclave import workflow


def protocol():

	cols_in_one = [
		defCol("car_id", "INTEGER", [1]),
		defCol("location", "INTEGER", [1])
	]
	in_one = create("in1", cols_in_one, {1})

	cols_in_two = [
		defCol("car_id", "INTEGER", [2]),
		defCol("location", "INTEGER", [2])
	]
	in_two = create("in2", cols_in_two, {2})

	cols_in_three = [
		defCol("car_id", "INTEGER", [3]),
		defCol("location", "INTEGER", [3])
	]
	in_three = create("in3", cols_in_three, {3})

	combined = concat([in_one, in_two, in_three], "combined_data")
	agged = aggregate_count(combined, "heatmap", ["location"], "by_location")
	collect(agged, 1)

	return {in_one, in_two, in_three}


if __name__ == "__main__":

	with open(sys.argv[1], "r") as c:
		c = json.load(c)

	workflow.run(protocol, c, mpc_framework="jiff", apply_optimisations=True)
```

*Note that each input file name must correspond to the name in the call to create(). 
So the input files for the example above would be called in1.csv, in2.csv, in3.csv. I've
included example input files at demo/data/ that were tested on this workflow.

In order to run the protocol, you'll need to add conclave to your PYTHONPATH as follows:
```shell script
export PYTHONPATH=$PYTHONPATH:<path to conclave on your machine>
```

In the above example, data from 3 parties is concatenated and then aggregated over the 
"location" column. The output is a simple count of cars per location value. Further 
documentation on conclave workflows can be found here: 
https://github.com/CCD-MPC/chamberlain/wiki/Workflows

Further documentation on the different queries available to conclave workflows 
is available here: https://github.com/CCD-MPC/chamberlain/wiki/Queries

Each party needs to submit it's own config file (which corresponds to sys.argv[1] 
in the above code). That config file should look like the following:


### config_one.json
```json
{
	"user_config":
	{
		"pid": <this_party's_id>,
		"all_pids": [<list of all party_id's in the computation'],
		"leaky_ops": 0,
		"workflow_name": "example",
		"use_floats": 1,
		"paths":
		{
			"input_path": "<path to parent directory of input file>",
		}
	},
	"net":
	{
		"parties": [
			{"host": "0.0.0.0", "port": 9001},
			{"host": "0.0.0.0", "port": 9002},
			{"host": "0.0.0.0", "port": 9003}
		]
	},
	"backends":
	{
		"jiff":
		{
			"available": 1,
			"jiff_path": "<path to jiff library>",
			"party_count": "<number of compute parties>",
			"server_ip": "0.0.0.0",
			"server_pid": 0,
			"server_port": 9000
		}
	}
}
```

In this example, the jiff server_pid value is 0 because it's assumed that party indexes start 
at 1, and the jiff server is being run by an independent party outside the computation. Also 
be sure to use a different input_path variable for each party in the computation, since that
directory will also be storing all of that party's generated code for the protocol.

You'll also need a JIFF server file for the computation, structured as follows:

### server.js
```javascript
var express = require('express');
var app = express();
var http = require('http').Server(app);

var JIFFServer = require('<path_to_jiff_library>//lib/jiff-server');
var jiff_instance = new JIFFServer(http, { logs: true });

var jiffBigNumberServer = require('<path_to_jiff_library>/lib/ext/jiff-server-bignumber');
jiff_instance.apply_extension(jiffBigNumberServer);

app.use("<path_to_jiff_library>//demos", express.static("demos"));
app.use("<path_to_jiff_library>//lib", express.static("lib"));
app.use("<path_to_jiff_library>//lib/ext", express.static("lib/ext"));
app.use("<path_to_jiff_library>/bignumber.js", express.static("node_modules/bignumber.js"));

http.listen(9000, function()
{
	console.log('listening on *:9000');
});
```

To start the Jiff server, open a terminal and run the following:

```shell script
export NODE_PATH="/Users/bengetchell/Desktop/dev/MPC/jiff/node_modules"
node server.js
```

Then, open a terminal window for each compute party, activate your virtualenv, and run the following:
```shell script
python protocol.py <path_to_config>.json
```

The computation will run, and the output should be located in the directory that you used
for input_path in the config file for party one. 
