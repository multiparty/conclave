# salmon

## Misc

(Incomplete) list of assumptions I'm making that I wish I wasn't:

* All workflows are written in such a way that each party inputs a relation, followed directly by a concat of all those relations.
A special case is when there is a join.

* All workflows are written so that there is exactly one output party that receives all outputs.

* Each child of a concat has at most one child.

...
