# Intermediate representation of dataflow graph 


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
            if tbl in schema.keys(): # base table 
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
            new_node = Node(node_name, "filter", self.affected_base_tables, self.policy, predicate=predicate)  
            graph[new_node] = []
            upstream = set()
            left, right = predicate.split('IN')

            if '.' in left and prev is None: 
                left_table, left_col = left.split('.')
                upstream.add(left_table.replace('$', '').strip())
                print('ADDING LEFT {} to upstream'.format(left))

            if '.' in right and prev is None: 
                right_table, right_col = right.split('.')
                upstream.add(right_table.replace('$', '').strip())
                print('ADDING RIGHT {} to upstream'.format(left))
            
            intermediate_views.append(new_node)

            if prev is not None: 
                upstream.add(prev.name)
                # print('adding prev to upstream: {}'.format(prev))

            prev = new_node 

            for tbl in upstream: 
                found = False
                for node in graph.keys(): 
                    # print('looking: {}'.format(node.name))
                    if node.name == tbl: 
                        graph[node].append(new_node)
                        found = True
                
                if not found: 
                    raise NotImplementedError
        print("GRAPH: {} INTERMEDIATE VIEWS: {}".format(graph, intermediate_views))
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