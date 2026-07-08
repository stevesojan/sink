from main.commons.common_constants import DATA, DF, ID, JSON_DICT, LINKED_LIST_JSON, LIST_OF_NODES_EXECUTED, ALGONAME, \
    MODEL_SEQUENCE, SERVING_SEQUENCE, SETTINGS, SETTINGS_JSON_KEY, STATUS_CODE, STATUS_DICT, POSTGRES_SETTINGS, RENAME, DROP
import requests
from main.commons.constants_main import FEATURE_SETTINGS, MODELALGOSETTINGS, SOURCE_SETTINGS, FEATURE_SETTINGS, \
    MODELALGOSETTINGS, SINK_SETTINGS, SOURCE_SETTINGS, GENAISETTINGS, CATEGORICAL_COLUMNS, ETL_SETTINGS

false=False
true=True
null=None

def linked_list_traversal(*args):
    head_node=args[0]; json_dict=args[1]; nodes_completed=args[2]; log_object=args[3]; flag=args[4]; global_dataframe=args[5]; model_sequence=args[6]; serving_sequence=args[7]; sink_node=args[8]; clustering_models=args[9]; upper_layer_object=args[10]; list_of_nodes_executed=args[11]; status_dict=args[12]; pipeline_graph=args[13]
    cluster=None
    updated_categorical_columns = None
    while(head_node != None):
        linked_list_json = head_node.json
        if head_node.json[SETTINGS] == SINK_SETTINGS:
            nodes_completed.append(head_node.id)
            sink_node=head_node
            update_categorical_columns_for_autml(json_dict,head_node,updated_categorical_columns)
            
            return flag, global_dataframe, model_sequence, serving_seqounce, sink_node
            
        setting_url=upper_layer_object.executioner(linked_list_json)
        clustering_node = cluster
        cluster = head_node.id if linked_list_json[ALGONAME] in clustering_models else clustering_node
        linked_list_json['cluster']=cluster
        
        
        dictionary_pass = {LINKED_LIST_JSON: linked_list_json, JSON_DICT: json_dict, DATA: global_dataframe,
                           MODEL_SEQUENCE: model_sequence, SERVING_SEQUENCE: serving_seqounce, "log_object": log_object,
                           'pipeline_graph': pipeline_graph}
                           
        update_dict_for_genai(linked_list_json,dictionary_pass,serving_seqounce)
        response = setting_url.object_creator(dictionary_pass)
        # During ETL in case of Drop and Rename the categorical list can be changed if operation is performed on them. Therefore, updating the columns after
        if head_node.json[SETTINGS] == ETL_SETTINGS and response [SERVING_SEQUENCE][JSON_DICT][head_node.id][ALGONAME] in [DROP, RENAME]:
            updated_categorical_columns = response [SERVING_SEQUENCE][JSON_DICT][head_node.id][CATEGORICAL_COLUMNS]
        api_response = response
        status_dict.update(api_response[STATUS_DICT])
        list_of_nodes_executed.extend(api_response[LIST_OF_NODES_EXECUTED])
        print("response --> ", response)
        if response [STATUS_CODE] == 200:
            remove_prev_df_add_latest_df(api_response, global_dataframe, linked_list_json, head_node)
            model_sequence= api_response["model_sequence"] if api_response["model_sequence"] is not None else model_sequence
            
            serving_seqounce = process_serving_sequence(serving_seqounce, api_response, linked_list_json)
            
        nodes_completed.append(head_node.id)
        if response[STATUS_CODE] ==500:
            return 0
        head_node = head_node.next_id
    return flag, global_dataframe, model_sequence, serving_seqounce, sink_node

def process_serving_sequence(serving_seqounce, api_response, linked_list_json):
    if api_response["serving_seqounce"]!=None and linked_list_json[ALGONAME] != 'genAi':
        print("serving sequence null")
        serving_seqounce.append(api_response[SERVING_SEQUENCE])
    elif api_response["serving_seqounce"]!=None and linked_list_json[ALGONAME] == 'genAi':
        serving_seqounce = api_response[SERVING_SEQUENCE]
        
    return serving_seqounce

def remove_prev_df_add_latest_df(api_response, global_dataframe, linked_list_json, head_node):
    current_id_to_map = api_response["id"]
    dataframe_to_map = api_response["df"]
    if dataframe_to_map is not None:
        if linked_list_json["algoName"]=="a" or linked_list_json["settings"] not in [SOURCE_SETTINGS,MODELALGOSETTINGS,FEATURE_SETTINGS,GENAISETTINGS]:
            for i in head_node.previous_id:
                prev_id = i.id
                try:
                    del(global_dataframe[prev_id])
                except Exception: pass
        global_dataframe[current_id_to_map] = dataframe_to_map


def update_categorical_columns_for_autml(json_dict, head_node, updated_categorical_columns):
    # Updating the categorical columns for AUTOML. Missing Value in AUTOML takes categorical column as input hence, need to update the categorical columns
    if updated_categorical_columns:
        json_dict[head_node.id][CATEGORICAL_COLUMNS] = updated_categorical_columns


def update_dict_for_genai(linked_list_json, dictionary_pass, serving_seqounce):
    if linked_list_json[ALGONAME] == 'genAi':
        dictionary_pass.update({"serving_sequence": serving_seqounce})