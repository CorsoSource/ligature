from shared.data.update import UpdateModel

import sys,os
import networkx as nx
import nxpd
nxpd.nxpdParams['show'] = 'ipynb'
os.environ['PATH'] += os.pathsep + '/opt/conda/envs/py27/bin'

class GraphModel(UpdateModel):
    
    # Global graph of the calculation
    graph = nx.DiGraph()
    
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
        return nxpd.draw(self.graph)
    
    def subscribe(self, listener):
        super(GraphModel, self).subscribe(listener)
        self.graph.add_edge(self, listener)
    
    def unsubscribe(self, listener):
        super(GraphModel, self).unsubscribe(listener)
        self.graph.remove_edge(self, listener)