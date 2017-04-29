
class Dag():

    def __init__(self, roots):
        self.roots = roots

    def _dfsVisit(self, node, visitor, visited):
        visitor(node)
        visited.add(node)
        for child in node.children:
            if child not in visited:
                self._dfsVisit(child, visitor, visited)

    def dfsVisit(self, visitor):
        visited = set()

        for root in self.roots:
            self._dfsVisit(root, visitor, visited)

    def dfsPrint(self):
        self.dfsVisit(print)

class Node():

    def __init__(self, name, children=None, parents=None):
        self.name = name
        self.children = children if children else set()
        self.parents = parents if parents else set()

    def addChild(self, child):
        self.children.add(child)
        child.parents.add(self)

    def addParent(self, parent):
        self.parents.add(parents)
        parent.children.add(self)

    def debugStr(self):
        childrenStr = str([n.name for n in self.children])
        parentStr = str([n.name for n in self.parents]) 
        return self.name + " children: " + childrenStr + " parents: " + parentStr 

    def __str__(self):
        return self.name

class OperatorNode():

    pass

if __name__ == "__main__":
    a = Node("a")
    b = Node("b")
    c = Node("c")
    d = Node("d")
    e = Node("e")

    a.addChild(b)
    a.addChild(c)

    b.addChild(c)
    b.addChild(d)

    c.addChild(d)

    e.addChild(d)

    dag = Dag(set([a, e]))
    dag.dfsPrint()
