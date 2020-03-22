# Intermediate representation of dataflow graph 


class Node: 
    def __init__(self, name, operation_type, policy, predicate=None, operation_on=None, groupby=None, exported_as=None): 
        self.name = name
        self.operation_type = operation_type
        self.predicate = predicate 
        self.operation_on = operation_on 
        self.groupby = groupby
        self.policy = policy
        self.exported_as = exported_as 
    
    def __repr__(self):
        return "< NODE: name: %s, optype: %s, predicate: %s>\n" % (self.name, self.operation_type, self.predicate)
    
    def check_commutativity(operations): 
        left, right = self.predicate.split('IN')
        left_table, left_col = left.split('.')
        right_table, right_col = right.split('.')


class Function: 
    def __init__(self, event_chain, schema): 
        self.event_chain = event_chain 
        self.schema = schema

    def __repr__(self):
        return "<Function: event chain: %s,\n schema: %s>\n" % (self.event_chain, self.schema)

    def to_dataflow(self, schema): 
        intermediate_views = []
        intermediate_graph = {}
        for operation in self.event_chain: 
            subgraph, output_views = operation.to_dataflow(schema, intermediate_graph, intermediate_views)
            intermediate_graph = subgraph 
            intermediate_views = output_views 

        return intermediate_graph

def get_node_by_name(graph, node_name): 
    for node in graph.keys(): 
        if node.name == node_name: 
            return node 
    return None 


class Filter: 
    def __init__(self, new_view_name, tables, predicates, policy=False, exported_as=None, on=False):
        self.new_view_name = new_view_name
        self.tables = tables
        self.predicates = predicates  
        self.policy = policy 
        self.exported_as = exported_as 
        self.on = on 
    
    def __repr__(self):
        return "< Filter: view name: %s,\n tables: %s,\n predicates: %s, policy: %s>\n" % (self.new_view_name, self.tables, self.predicates, self.policy)
    
    def to_dataflow(self, schema, graph, intermediate_views): 
        for tbl in self.tables: 
            tbl = tbl.replace('$', '')

            intermediate_view_names = [x.name for x in intermediate_views]
            in_intermediate = False
            for name in intermediate_view_names: 
                if tbl == name: 
                    in_intermediate = True
            if tbl in schema.keys(): # base table 
                tbl_node = Node(tbl, None, [tbl], self.policy)  
                if tbl_node not in graph: 
                    graph[tbl_node] = []  
            elif in_intermediate: 
                tbl_node = intermediate_views[intermediate_view_names.index(tbl)]
                if tbl_node not in graph: 
                    graph[tbl_node] = []  
            else: 
                raise NotImplementedError
    
        if len(self.predicates) == 0: 
            new_node = Node(self.new_view_name, "filter", self.policy, predicate=None, exported_as=self.exported_as)
            graph[new_node] = []
            intermediate_views.append(new_node)
            left, right = self.tables
            left_node = get_node_by_name(graph, left)
            right_node = get_node_by_name(graph, right)
            graph[left_node].append(new_node)
            graph[right_node].append(new_node)

        prev = None
        for i, predicate in enumerate(self.predicates): 
            if i == len(self.predicates) - 1:
                i = ""
            node_name = self.new_view_name + str(i)
            new_node = Node(node_name, "filter", self.policy, predicate=predicate, exported_as=self.exported_as)  
            graph[new_node] = []
            upstream = set()
            left, right = predicate.split('IN')

            print("PREDICATE: {}".format(predicate))
            if '.' in left and prev is None: 
                print('h1')
                left_table, left_col = left.split('.')
                upstream.add(left_table.replace('$', '').strip())

            if '.' in right and prev is None: 
                print('h2')
                right_table, right_col = right.split('.')
                upstream.add(right_table.replace('$', '').strip())
            
            intermediate_views.append(new_node)

            if prev is not None: 
                upstream.add(prev.name)
                # print('adding prev to upstream: {}'.format(prev))

            prev = new_node 

            if i == 0: 
                for table in self.tables: 
                    upstream.add(table) 

            for tbl in upstream: 
                found = False
                for node in graph.keys(): 
                    # print('looking: {}'.format(node.name))
                    if node.name == tbl: 
                        print('appending {} to node {} outgoing edges'.format(new_node, upstream))
                        graph[node].append(new_node)
                        found = True
                
                if not found: 
                    raise NotImplementedError

        return graph, intermediate_views


class Transform: 
    def __init__(self, new_view_name, tables, predicates, policy=False, exported_as=None):
        self.new_view_name = new_view_name
        self.tables = tables
        self.predicates = predicates 
        self.policy = policy
        self.exported_as = exported_as 
    
    def __repr__(self):
        return "<Transform: view name: %s,\n tables: %s,\n predicates: %s, policy: %s>\n" % (self.new_view_name, self.tables, self.predicates, self.policy)

    def to_dataflow(self, schema, graph, intermediate_views): 
        for tbl in self.tables: 
            tbl = tbl.replace('$', '')
            intermediate_view_names = [x.name.replace('$', '') for x in intermediate_views]
            in_intermediate = False
            for name in intermediate_view_names: 
                if tbl == name: 
                    in_intermediate = True
            if tbl in schema.keys(): # base table 
                tbl_node = Node(tbl, None, [tbl], self.policy, exported_as=self.exported_as)
                if tbl_node not in graph: 
                    graph[tbl_node] = []  
            elif in_intermediate: 
                tbl_node = intermediate_views[intermediate_view_names.index(tbl)]
                if tbl_node not in graph: 
                    graph[tbl_node] = []  
            else: 
                raise NotImplementedError
    
        prev = None
        prev_connected = set()
        for i, predicate in enumerate(self.predicates): 
            if i == len(self.predicates) - 1:
                i = ""
            node_name = self.new_view_name + str(i)
            new_node = Node(node_name, "transform", self.policy, predicate=predicate, exported_as=self.exported_as)  
            graph[new_node] = []
            upstream = set()
            
            print("predicate: {}".format(predicate))
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

            if i == 0: 
                for table in self.tables: 
                    upstream.add(table)

            prev = new_node 
            
            new_upstream = set()
            for item in upstream: 
                if item not in prev_connected: 
                    new_upstream.add(item)
            
            for tbl in new_upstream: 
                found = False
                for node in graph.keys(): 
                    if node.name == tbl: 
                        graph[node].append(new_node)
                        found = True
                
                if not found: 
                    raise NotImplementedError

            for t in new_upstream: 
                prev_connected.add(t)
                    
        return graph, intermediate_views


class Aggregate: 
    def __init__(self, new_view_name, operation_type, tables, operation_on, predicates, groupby=None, policy=False, exported_as=None):
        self.new_view_name = new_view_name
        self.operation_type = operation_type 
        self.operation_on = operation_on 
        self.tables = tables
        self.predicates = predicates 
        self.groupby = groupby 
        self.policy = policy 
        self.exported_as = exported_as 
    
    def __repr__(self):
        return "<Aggregate: op type: %s,\n op on: %s,\n tables: %s,\n predicates: %s,\n groupby: %s, policy: %s>\n" % (self.operation_type, self.operation_on, self.tables, self.predicates, self.groupby, self.policy)

    def to_dataflow(self, schema, graph, intermediate_views): 
        for tbl in self.tables: 
            tbl = tbl.replace('$', '')

            intermediate_view_names = [x.name for x in intermediate_views]
            if tbl in schema.keys():# base table 
                tbl_node = Node(tbl, None, [tbl], self.policy) 
                found = False 
                for node in graph.keys(): 
                    if tbl_node.name == node.name: 
                        found = True
                if not found: 
                    graph[tbl_node] = []  
            elif tbl in intermediate_view_names: 
                tbl_node = intermediate_views[intermediate_view_names.index(tbl)]
                found = False 
                for node in graph.keys(): 
                    if tbl_node.name == node.name: 
                        found = True
                if not found: 
                    graph[tbl_node] = []  
            else: 
                raise NotImplementedError
    
        if '.' in self.operation_on: 
            node_name = self.new_view_name
            new_node = Node(node_name, self.operation_type, self.policy, operation_on=self.operation_on, groupby=self.groupby, exported_as=self.exported_as)  
            graph[new_node] = []
                    
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
                node = Node(node_name, self.operation_type, self.affected_base_tables, self.policy, predicate=predicate, exported_as=self.exported_as)  
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
