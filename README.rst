========
conclave
========

Infrastructure for defining and running large data workflows against multiple backends.

Purpose
-------
This framework allows users to define data analysis workflows in familiar frontend languages and then execute them on multiple data storage and processing backends (including privacy-preserving backend services that support secure multi-party computation).

Dependencies
------------

Conclave requires a Python 3.x environment. On Ubuntu (14.04+), installing the `python3`, `python3-pystache` should get everything that's needed.

Testing
-------

The library comes with a number of tests::

    nosetests

Assumptions
-----------

* All workflows are written in such a way that each party inputs a relation, followed directly by a concat of all those relations. A special case is when there is a join.
* All workflows are written so that there is exactly one output party that receives all outputs.
* Each child of a concat has at most one child.
