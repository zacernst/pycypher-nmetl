import networkx as nx

edge_dictionary = {
    "a": ["b", "c", "d", "e"],
    "b": ["a", "e"],
    "c": ["a", "d"],
    "d": ["a", "c"],
    "e": ["a", "b"],
    "f": ["g"],
    "g": ["f"],
}

graph = nx.DiGraph(edge_dictionary)

print(nx.info(graph))
