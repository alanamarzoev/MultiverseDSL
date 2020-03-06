import os 
import sys
import json 
import networkx as nx
import matplotlib.pyplot as plt
from networkx.drawing.nx_agraph import graphviz_layout
from moz_sql_parser import parse
from enum import Enum


class Filter: 
    def __init__(self, new_view_name, tables, predicates, affected_base_tables=None, policy=False):
        self.new_view_name = new_view_name
        self.tables = tables
        self.predicates = predicates 
        self.policy = policy 
        self.affected_base_tables = affected_base_tables
    
    def __repr__(self):
        return "< Filter: view name: %s,\n tables: %s,\n predicates: %s, policy: %s>\n" % (self.new_view_name, self.tables, self.predicates, self.policy)
    
    def to_dataflow(self, schema, graph, intermediate_views): 
        print('\n\n\n\n {}'.format(self))
        for tbl in self.tables: 
            tbl = tbl.replace('$', '')

            intermediate_view_names = [x.name for x in intermediate_views]
            if tbl in schema.keys():# base table 
                tbl_node = Node(tbl, None, [tbl])  
                graph[tbl_node] = []  
            elif tbl in intermediate_view_names: 
                tbl_node = intermediate_views[intermediate_view_names.index(tbl)]
                graph[tbl_node] = []  
            else: 
                raise NotImplementedError
    
        prev = None
        for i, predicate in enumerate(self.predicates): 
            if i == len(self.predicates) - 1:
                i = ""
            node_name = self.new_view_name + str(i)
            new_node = Node(node_name, "filter", self.affected_base_tables, predicate)  
            graph[new_node] = []
            upstream = set()
            left, right = predicate.split('IN')
            if '.' in left: 
                left_table, left_col = left.split('.')
                upstream.add(left_table.replace('$', '').strip())

            if '.' in right: 
                right_table, right_col = right.split('.')
                upstream.add(right_table.replace('$', '').strip())
            
            intermediate_views.append(new_node)

            if prev is not None: 
                upstream.add(prev.name)
                print('adding prev to upstream: {}'.format(prev))

            prev = new_node 

            for tbl in upstream: 
                found = False
                for node in graph.keys(): 
                    print('looking: {}'.format(node.name))
                    if node.name == tbl: 
                        graph[node].append(new_node)
                        found = True
                
                if not found: 
                    raise NotImplementedError
                    
        return graph, intermediate_views


class Node: 
    def __init__(self, name, operation_type, basetables, policy, predicate=None, operation_on=None, groupby=None): 
        self.name = name
        self.operation_type = operation_type
        self.predicate = predicate 
        self.operation_on = operation_on 
        self.groupby = groupby
        self.basetables = basetables
        self.policy = policy
    
    def __repr__(self):
        return "< NODE: name: %s, optype: %s, basetables: %s, predicate: %s>\n" % (self.name, self.operation_type, self.basetables, self.predicate)
    
    def check_commutativity(operations): 
        left, right = self.predicate.split('IN')
        left_table, left_col = left.split('.')
        right_table, right_col = right.split('.')


class Transform: 
    def __init__(self, new_view_name, tables, predicates, affected_base_tables=None, policy=False):
        self.new_view_name = new_view_name
        self.tables = tables
        self.predicates = predicates 
        self.policy = policy
        self.affected_base_tables = affected_base_tables
    
    def __repr__(self):
        return "<Transform: view name: %s,\n tables: %s,\n predicates: %s, policy: %s>\n" % (self.new_view_name, self.tables, self.predicates, self.policy)

    def to_dataflow(self, schema, graph, intermediate_views): 
        print('\n\n\n\n {}'.format(self))
        for tbl in self.tables: 
            tbl = tbl.replace('$', '')
            intermediate_view_names = [x.name.replace('$', '') for x in intermediate_views]
            # print("TABLE: {} intermediate views: {} schema: {}".format(tbl, intermediate_view_names, schema))
            if tbl in schema.keys():# base table 
                tbl_node = Node(tbl, None, [tbl], self.policy) 
                graph[tbl_node] = []  
            elif tbl in intermediate_view_names: 
                tbl_node = intermediate_views[intermediate_view_names.index(tbl)]
                graph[tbl_node] = []  
            else: 
                raise NotImplementedError
    
        prev = None
        for i, predicate in enumerate(self.predicates): 
            if i == len(self.predicates) - 1:
                i = ""
            node_name = self.new_view_name + str(i)
            new_node = Node(node_name, "transform", self.affected_base_tables, self.policy, predicate=predicate)  
            graph[new_node] = []
            upstream = set()
            
            if 'IN' in predicate: 
                left, right = predicate.split('IN')
                if '.' in left: 
                    left_table, left_col = left.split('.')
                    upstream.add(left_table.replace('$', '').strip())

                if '.' in right: 
                    right_table, right_col = right.split('.')
                    upstream.add(right_table.replace('$', '').strip())
            elif '=>' in predicate: 
                left, right = predicate.split('=>')
                table, col = left.split('.')
                table = table.replace('$', '').strip()
                upstream.add(table)
                
            intermediate_views.append(new_node)

            if prev is not None: 
                upstream.add(prev.name)
                print('adding prev to upstream: {}'.format(prev))

            prev = new_node 

            for tbl in upstream: 
                print('tbl: {}'.format(tbl))
                found = False
                for node in graph.keys(): 
                    print('looking: {}'.format(node.name))
                    if node.name == tbl: 
                        graph[node].append(new_node)
                        found = True
                
                if not found: 
                    raise NotImplementedError
                    
        return graph, intermediate_views


class Aggregate: 
    def __init__(self, new_view_name, operation_type, tables, operation_on, predicates, affected_base_tables=None, groupby=None, policy=False):
        self.new_view_name = new_view_name
        self.operation_type = operation_type 
        self.operation_on = operation_on 
        self.tables = tables
        self.predicates = predicates 
        self.groupby = groupby 
        self.affected_base_tables = affected_base_tables
        self.policy = policy 
    
    def __repr__(self):
        return "<Aggregate: op type: %s,\n op on: %s,\n tables: %s,\n predicates: %s,\n groupby: %s, policy: %s>\n" % (self.operation_type, self.operation_on, self.tables, self.predicates, self.groupby, self.policy)

    def to_dataflow(self, schema, graph, intermediate_views): 
        for tbl in self.tables: 
            tbl = tbl.replace('$', '')

            intermediate_view_names = [x.name for x in intermediate_views]
            if tbl in schema.keys():# base table 
                tbl_node = Node(tbl, None, [tbl], self.policy) 
                graph[tbl_node] = []  
            elif tbl in intermediate_view_names: 
                tbl_node = intermediate_views[intermediate_view_names.index(tbl)]
                graph[tbl_node] = []  
            else: 
                raise NotImplementedError
    
        if '.' in self.operation_on: 
            print("AGGREGATE HERE {}".format(self.new_view_name))
            node_name = self.new_view_name
            new_node = Node(node_name, self.operation_type, self.affected_base_tables, self.policy, operation_on=self.operation_on, groupby=self.groupby)  
            graph[new_node] = []
            print("AGGREGATE NODE: ")
            tbl, col = self.operation_on.split('.') 
            found = False 
            for node in graph.keys(): 
                if node.name == tbl: 
                    graph[node].append(new_node)
                    found = True
            intermediate_views.append(new_node)

        if self.predicates is not None: 
            raise NotImplementedError
            for i, predicate in enumerate(self.predicates): 
                if i == len(self.predicates) - 1:
                    i = ""
                node_name = self.new_view_name + str(i)
                node = Node(node_name, self.operation_type, self.affected_base_tables, self.policy, predicate=predicate)  
                graph[node] = []
                upstream = set()
                left, right = predicate.split('IN')
                if '.' in left: 
                    left_table, left_col = left.split('.')
                    upstream.add(left_table.replace('$', '').strip())

                if '.' in right: 
                    right_table, right_col = right.split('.')
                    upstream.add(right_table.replace('$', '').strip())
                
                intermediate_views.append(node)
                
                for tbl in upstream: 
                    found = False
                    for node in graph.keys(): 
                        if node.name == tbl: 
                            graph[node].append(node_name)
                            found = True
                    
                    if not found: 
                        raise NotImplementedError
        
                    
        return graph, intermediate_views


class Function: 
    def __init__(self, event_chain, schema): 
        self.event_chain = event_chain 
        self.schema = schema

    def __repr__(self):
        return "<Function: event chain: %s,\n schema: %s>\n" % (self.event_chain, self.schema)

    def to_dataflow(self, schema): 
        print(self.event_chain)
        intermediate_views = []
        intermediate_graph = {}
        for operation in self.event_chain: 
            subgraph, output_views = operation.to_dataflow(schema, intermediate_graph, intermediate_views)
            intermediate_graph = subgraph 
            intermediate_views = output_views 
            print("intermediate graph: {}".format(intermediate_graph))

        return intermediate_graph


def load_policies(schema): 
    my_submitted_reviews = Filter("MySubmittedReviews", 
                                 ["PaperReview"], 
                                 ["$UID IN PaperReview.contactId"],
                                 affected_base_tables=["PaperReview"]) 

    visible_reviews_unanonymized = Filter("VisibleReviews", 
                                        ["PaperReview"], 
                                        ["PaperReview.paperId IN MySubmittedReviews"], 
                                        affected_base_tables=["PaperReview"])

    visible_reviews_anonymized = Transform("VisibleReviewsAnonymized", 
                                        ["VisibleReviews"], 
                                        ["VisibleReviews.contactID => `anonymous`"],
                                        affected_base_tables=["PaperReview"])

    event_chain = [my_submitted_reviews, visible_reviews_unanonymized, visible_reviews_anonymized]

    reviewer_policy = Function(event_chain, schema) 
    reviewer_policy = reviewer_policy.to_dataflow(schema)

    return [reviewer_policy]


def load_schema(): 
    base_tables = {}
    f = open('../benchmarks/hotcrp/schema.sql')
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
        

def load_queries(schema):
    generic_query = "SELECT * FROM Paper;"
    reviewer_query = "SELECT Paper, PaperConflict, PaperReview \
                FROM Paper \
                LEFT JOIN PaperConflict \
                ON (PaperConflict.paperId=Paper.paperId AND PaperConflict.contactId=$UID) \
                LEFT JOIN PaperReview \
                ON (PaperReview.paperId=Paper.paperId AND PaperReview.contactId=$UID) \
                LEFT JOIN (SELECT paperId, count(*) FROM PaperReview \
                    GROUP BY paperID) R_submitted \
                ON (R_submitted.paperId=Paper.paperId);" 
    
    paper_join_paperconflict = Filter("Paper_PaperConflict",
                                     ["Paper", "PaperConflict"], 
                                     ["PaperConflict.paperId IN Paper.paperId", 
                                      "$UID IN PaperConflict.contactID"],
                                     policy=False)

    paper_paperconflict_join_review = Filter("PaperPaperConflict_PaperReview", 
                                             ["$Paper_PaperConflict", "PaperReview"],
                                             ["PaperReview.paperId IN Paper_PaperConflict.paperId", "PaperReview.contactId IN $UID"],
                                              policy=False)
    
    r_submitted =  Aggregate("R_submitted",
                             "count(*)", 
                             ["PaperReview"], 
                             "PaperReview.paperId",
                             None, 
                             groupby="PaperReview.paperId", 
                             policy=False)

    final_join = Filter("Final", 
                        ["PaperPaperConflict_PaperReview", "R_submitted"], 
                        ["R_submitted.paperId IN Paper.paperId"], 
                        policy=False)

    full_query = [paper_join_paperconflict, paper_paperconflict_join_review, r_submitted, final_join]
   
    full_query = Function(full_query, schema)
    full_query = full_query.to_dataflow(schema)

    return [full_query] 


def check_commutativity(op1, op2): 
    return


def planning(queries, policies): 
    # figure out what policy nodes apply to each basetable (preserving their order)
    basetable_to_policies = {}
    for policy in policies: 
        for node, edges in policy.items(): 
            print('node: {}'.format(node))
            for tbl in node.basetables: 
                if tbl not in basetable_to_policies: 
                    basetable_to_policies[tbl] = []
                basetable_to_policies[tbl].append(node)


    # # insert policy nodes directly below basetables, prior to any query computation nodes.
    # # this configuration will always be correct but it is clearly not optimal.
    # for query in queries: 
    #     for basetable, pols in basetable_to_policies.items(): 
    #         connected = query[basetable]
    #         if len(pols) > 0: 
    #             query[basetable] = pols[0]
    #             last = pols[0]
    #             for pol in range(1, len(pols)): 
    #                 query[pols[0]] = [pol] 
    #                 last = pol
    #             query[last] = connected 
    
    # now, our goal is to push the policy nodes as far down in the graph as possible.
    # we do this by comparing every policy node and its neighbor and seeing if we can 
    # swap their positions (aka, if the operations commute). we stop once we've reached 
    # a fixed point. TODO what happens at a branching point? in this case, it's no longer
    # necessarily better to push down the policy node. initial heuristic: if the node is
    # user dependent, continue to push it down, otherwise don't. TODO include the branching
    # factor in this cost model?
    base_to_pol = basetable_to_policies.copy()
    for query in queries: 
        for basetable, pol in base_to_pol.items(): 
            connected = base_to_pol[basetable]
            for node, out_edges in query.items(): 
    




def visualize(graph): 
    G = nx.DiGraph()

    for node in graph.keys(): 
        print('node: {}'.format(node))
        if node.operation_type == None: 
            G.add_node(node.name)

    for node, edges in graph.items():
        for edge in edges: 
            print("adding edge: {}".format(edge.name))
            G.add_node(edge.name)
            G.add_edge(node.name, edge.name)   
    
    pos = graphviz_layout(G, prog='dot')
    nx.draw(G, pos, with_labels=True, arrows=True)
    
    plt.show()


def main():
    schema = load_schema()
    queries = load_queries(schema) 
    policies = None 
    policies = load_policies(schema) 
    final_graph = planning(queries, policies)



if __name__ == '__main__': 
    main() 