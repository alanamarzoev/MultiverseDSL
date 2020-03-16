import argparse 
import benchmarks 
import os 
import planning 
import sys
import json 
import networkx as nx
import matplotlib.pyplot as plt
from benchmarks import * 
from networkx.drawing.nx_agraph import graphviz_layout
from moz_sql_parser import parse
from enum import Enum


def load_schema(schema_path): 
    base_tables = {}
    f = open(schema_path)
    lines = f.readlines()
    most_recent_table = None 
    for line in lines: 
        if 'CREATE' in line: 
            table_name = line.split('TABLE')[1].replace('(', '').replace('`', '').replace('\n', '')
            most_recent_table = table_name.strip()
            base_tables[most_recent_table] = []
        elif len(line.strip()) > 0 and line.strip()[0] == '`': 
            line = line.split()
            col_name = line[0].replace('`', '')
            base_tables[most_recent_table].append(col_name)
        else: 
            continue 

    return base_tables 


def load_policies(schema, benchmark): 
    if benchmark == 'hotcrp': 
        event_chain = hot_crp_event_chain 
    elif benchmark == 'twitter': 
        event_chain = twitter_event_chain 
    else: 
        raise NotImplementedError
    full_policy = Function(event_chain, schema) 
    full_policy = full_policy.to_dataflow(schema)

    return [full_policy]


def load_queries(schema, benchmark):
    if benchmark == 'hotcrp': 
        full_query = hot_crp_full_query
    elif benchmark == 'twitter': 
        full_query = twitter_full_query  
    else: 
        raise NotImplementedError

    full_query = Function(full_query, schema)
    full_query = full_query.to_dataflow(schema)
    return [full_query] 

 
def visualize(graph): 
    G = nx.DiGraph()

    for node in graph.keys(): 
        if node.operation_type == None: 
            G.add_node(node.name)

    for node, edges in graph.items():
        for edge in edges: 
            G.add_node(edge.name)
            G.add_edge(node.name, edge.name)   
    
    pos = graphviz_layout(G, prog='dot')
    nx.draw(G, pos, with_labels=True, arrows=True)
    
    plt.show()


def main():
    parser = argparse.ArgumentParser(description='Select benchmark.')
    parser.add_argument('--benchmark', type=str, default='hotcrp') 
    args = parser.parse_args()

    if args.benchmark == 'hotcrp': 
        schema_path = '../benchmarks/hotcrp/schema.sql'
    elif args.benchmark == 'twitter': 
        schema_path = '../benchmarks/twitter/schema.sql'
    else: 
        raise NotImplementedError 

    schema = load_schema(schema_path)
    queries = load_queries(schema, args.benchmark)
    visualize(queries[0]) 
    policies = load_policies(schema, args.benchmark)
    visualize(policies[0])
    final_graph = planning(queries, policies)
    print('final graph: {}'.format(final_graph))
    # for graph in final_graph: 
        # visualize(graph) 


if __name__ == '__main__': 
    main() 