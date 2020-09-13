from .update import UpdateModel

try:
    from networkx import DiGraph
    from nxpd import draw
    
    try:
        if get_ipython().__class__.__name__ == 'ZMQInteractiveShell':
            nxpd.nxpdParams['show'] = 'ipynb'
    except NameError:
        pass
    
    import distutils.spawn, os
    try:
        for binName in ('dot', 'dot.exe'):
            dotPath = distutils.spawn.find_executable(binName)
            if not dotPath:
                continue
            dotPath = dotPath.rpartition('/')[0]
            if not dotPath in os.environ['PATH']:
                os.environ['PATH'] += os.pathsep + dotPath
            break
    except:
        pass

except ImportError:

    class PlaceholderDiGraph(object):
        """
        Placeholder for graph lib. 
        """
        def __init__(self):
            self.nodes = {}
            self.edges = set()
            
        def __iter__(self):
            for node in self.nodes:
                yield node
        
        def __repr__(self):
            return '<PH Digraph - %d nodes and %d edges>' % (len(self.nodes), len(self.edges))
                    
        def add_node(self, node):
            self.nodes[node] = {}
            
        def remove_node(self, node):
            if node in self.nodes:
                del self.nodes[node]
                self.edges = set(edge for edge in self.edges if not node in edge)
                            
        def add_edge(self, start, end):
            if not start in self.nodes:
                self.add_node(start)
            if not end   in self.nodes:
                self.add_node(end)
                
            self.edges.add((start,end))
            
            self.nodes[start]['edges'] = 1 + self.nodes[start].get('edges', 0)
            self.nodes[end  ]['edges'] = 1 + self.nodes[end  ].get('edges', 0)
            
        def remove_edge(self, start, end):
            try:
                self.edges.remove((start, end))
                
                self.nodes[start]['edges'] = self.nodes[start].get('edges', 0) - 1
                self.nodes[end  ]['edges'] = self.nodes[end  ].get('edges', 0) - 1
    
            except KeyError:
                pass
            
            if start in self.nodes and self.nodes[start].get('edges', 0) <= 0:
                del self.nodes[start]
            if end   in self.nodes and self.nodes[end].get('edges', 0) <= 0:
                del self.nodes[end]

    DiGraph = PlaceholderDiGraph
    
    def draw(*args, **kwargs):
        raise NotImplementedError("To draw update graph, ensure nxpd is installed (NetworkX and Graphviz's Dot)")
                

class GraphModel(UpdateModel):
    
    # Global graph of the calculation
    graph = DiGraph()
    
    def __init__(self, *args, **kwargs):
        # Initialize mixins
        super(GraphModel, self).__init__(*args, **kwargs)
        self.graph.add_node(self)
    
    def _graph_attributes(self):
        """Define the attributes of this node for graphing.
           Override in the subclasses to customize the graph nodes.
        """
        return {}
    
    def draw_graph(self):
        for node in self.graph:
            for key,value in node._graph_attributes().items():
                self.graph.nodes[node][key] = value
        return draw(self.graph)
            
    def subscribe(self, listener):
        super(GraphModel, self).subscribe(listener)
        self.graph.add_edge(self, listener)
    
    def unsubscribe(self, listener):
        super(GraphModel, self).unsubscribe(listener)
        self.graph.remove_edge(self, listener)