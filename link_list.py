from main.commons.common_constants import CURRENTID, NEXTNODEID, PERVIOUSID, SETTINGS_JSON_KEY, ALGONAME, GENAI_SETTINGS
from main.commons import constants_main
from main.utils.JsonParser import CreateNodeStructure

class Node:
    def __init__(self, dataval=None, next_id=None, json=None, previous_id=None):
        self.id = dataval
        self.next_id = next_id
        self.json = json
        self.nextval = None
        self.previous_id = previous_id

class LinkedList:
    def __init__(self):
        self.headval = None
        
    def create_list(self, node_list, dict_of_nodes, json_dict):
        if node_list == None:
            return None
        list_of_nodes = []
        for nextnode in node_list.split(','):
            if nextnode in json_dict:
                if nextnode not in dict_of_nodes:
                    temp = Node(nextnode)
                    dict_of_nodes[nextnode] = temp
                list_of_nodes.append(dict_of_nodes[nextnode])
        return list_of_nodes

    def linked_list_creation(self, json_val, pipeline_graph, json_dict):
        head_node = self.headval
        dict_of_nodes = {}
        for nodes in json_val["linkNodeOfNAIComponents"]:
            node_id = nodes[CURRENTID]
            if node_id not in dict_of_nodes:
                temp = Node(node_id)
                dict_of_nodes[node_id] = temp
            current_node = dict_of_nodes[node_id]
            create_node = CreateNodeStructure
            create_node.create_graph_node(json_dict, node_id, pipeline_graph, nodes['nextNodeId'])
            next_node_list = nodes[NEXTNODEID]
            prev_node_list = nodes[PERVIOUSID]
            next_list = self.create_list(next_node_list, dict_of_nodes, json_dict)
            prev_list = self.create_list(prev_node_list, dict_of_nodes, json_dict)
            current_node.next_id = next_list
            current_node.previous_id = prev_list
            current_node.json = nodes
            if nodes[SETTINGS_JSON_KEY] in ["sourceSettings", "featureSettings"] and head_node == None:
                head_node = current_node
            elif nodes[SETTINGS_JSON_KEY] in [GENAI_SETTINGS] and next_node_list:
                head_node = current_node
                current_node.previous_id = None
        self.headval = head_node
        self.update_head_node(head_node, 0, [], {0: 0}, "sourceSettings")
        sink_node = {}
        last_node = self.rearrange(None, [head_node], [], sink_node)
        if '-1' in json_dict:
            last_node = self.create_rule_node(json_dict, last_node)
        if sink_node :
            sink_node[0].previous_id = last_node
            last_node.next_id = sink_node[0]
            last_node = sink_node[0]
        last_node.next_id = None
        return head_node

    def create_rule_node(self, json_dict, last_node):
        # Unused parameters
        _ = json_dict
        
        rule_node = Node('-1')
        rule_node.previous_id = last_node
        rule_setting_map = {}
        rule_setting_map['currentId'] = '-1'
        rule_setting_map['settings'] = constants_main.DROOLS_SETTINGS_JSON_PROPERTY
        rule_setting_map[ALGONAME] = "drools"
        rule_node.json = rule_setting_map
        last_node.next_id = rule_node
        return rule_node
    
    def update_head_node(self, head_node: Node, depth: int, nodes_executed: list, source_depth, setting_type):
        if head_node == None:
            return
        nodes_executed.append(head_node)
        prevnode = head_node.previous_id
        nextnode = head_node.next_id
        nodes_executed.append(head_node)
        if head_node != None and head_node.json != None and head_node.json[SETTINGS_JSON_KEY] == setting_type and depth < source_depth[0]:
            self.headval = head_node
            source_depth[0] = depth
        if prevnode != None:
            for node in prevnode:
                if node not in nodes_executed:
                    self.update_head_node(node, depth-1, nodes_executed, source_depth, setting_type)
        if nextnode != None:
            for node in nextnode:
                if node not in nodes_executed:
                    self.update_head_node(node, depth+1, nodes_executed, source_depth, setting_type)


    def rearrange(self, head_node, stack, list_of_visited_nodes, sink_node):
        while stack:
            current_node: Node = stack.pop()
            if current_node != None and current_node.json != None and current_node.json['settings'] == 'sinkSettings':
                sink_node[0] = current_node
                continue
            list_of_visited_nodes.append(current_node)
            prevnode = current_node.previous_id
            head_node = self.check_and_visit_prev_nodes(prevnode, list_of_visited_nodes, sink_node, head_node)
            nextnode = current_node.next_id
            if nextnode != None:
                for node in reversed(nextnode):
                    if node not in list_of_visited_nodes:
                        stack.append(node)
            if head_node != None:
                head_node.next_id = current_node
            head_node = current_node
        return head_node
    
    def check_and_visit_prev_nodes(self, prevnode, list_of_visited_nodes, sink_node, head_node):
        if prevnode != None:
            for node in prevnode:
                if node not in list_of_visited_nodes:
                    head_node = self.rearrange(head_node, [node], list_of_visited_nodes, sink_node)
        return head_node