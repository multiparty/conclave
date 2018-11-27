## Conclave

[Conclave](https://github.com/multiparty/conclave) is a query compiler and execution environment for relational queries across data sets owned by multiple, mutually distrusting parties. Executing such queries without revealing any cleartext data requires *Secure Multi-Party Computation* (MPC), and Conclave is the first system to support interactive queries that return in minutes over large input data sets.

Conclave automatically transforms SQL-like queries into a series of local and secure computation steps such that the costly secure computation under MPC's cryptographic guarantees is minimized. In doing so, Conclave scales to large datasets, but yet still meets the security guarantees of MPC. Conclave often returns results in minutes for queries that take other MPC systems hours or day to evaluate.

<p style="border: 1px solid darkred; color: darkred; font-size: 0.9em; font-weight: bold;">
We expect to release a preprint of the research paper on Conclave in early December 2018.
If you would like to find out more or get an advance copy, please [contact us by email](mailto:conclave@multiparty.org).
</p>

### Code

Our prototype is open-source [on Github](https://github.com/multiparty/conclave), and it currently supports MPC computation using the [Sharemind](https://sharemind.cyber.ee/) and [Obliv-C](https://oblivc.org/) systems.

### Authors

Conclave is a joint research project between Boston University and MIT CSAIL.
The contributing researchers are:

* [Nikolaj Volgushev](https://n1v0lg.github.io/)
* [Malte Schwarzkopf](http://people.csail.mit.edu/malte/)
* [Ben Getchell](https://www.bu.edu/hic/profile/ben-getchell/)
* [Andrei Lapets](https://cs-people.bu.edu/lapets/)
* [Mayank Varia](https://www.mvaria.com/)
* [Azer Bestavros](http://azer.bestavros.net/)
