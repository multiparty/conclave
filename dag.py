
class Column():

    def __init__(self, relName, typeStr, colSet):
        
        self.relName = relName
        self.typeStr = typeStr
        self.colSet = colSet

class Relation():

    def __init__(self, name, columns):
        
        self.columns = columns
        self.name = name

    def __str__(self):
        return self.name

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

class OpNode(Node):

    def __init__(self, name, inRels, outRel, children=None, parents=None):
        
        super(OpNode, self).__init__(name, children, parents)
        self.inRels = inRels
        self.outRel = outRel

    def __str__(self):
        
        inRelStr = " ".join([str(rel) for rel in self.inRels])
        return "{}, {}, {}".format(self.name, inRelStr, str(self.outRel))

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

class OpDag(Dag):

    def __init__(self, roots):
        
        super(OpDag, self).__init__(roots)


def create(name, columns):

    columns = [Column(name, typeStr, collusionSet) for typeStr, collusionSet in columns]
    outRel = Relation(name, columns)
    op = OpNode("create", inRels=[], outRel=outRel)
    return op

def simpleDagExample():

    a = OpNode("a", None, None)
    b = OpNode("b", None, None)
    c = OpNode("c", None, None)
    d = OpNode("d", None, None)
    e = OpNode("e", None, None)

    a.addChild(b)
    a.addChild(c)

    b.addChild(c)
    b.addChild(d)

    c.addChild(d)

    e.addChild(d)

    dag = OpDag(set([a, e]))
    dag.dfsPrint()

if __name__ == "__main__":

    colsRelA = [("int", set([1, 2, 3])), ("int", set([1, 2, 3]))]
    relA = create("relA", colsRelA)
    dag = OpDag(set([relA]))
    dag.dfsPrint()
    