
import requests
from fastapi import FastAPI, Request
import re
import asyncio
import threading
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from main import config_file
from main.commons.common_constants import ALGONAME, DATA, EMPTY_STRING, JSON_DICT, LINKED_LIST_JSON, \
    LIST_OF_NODES_EXECUTED, MODEL_SEQUENCE, PIPELINEID, SERVING_SEQUENCE, STATUS_CODE, STATUS_DICT, TIMESTAMP, VERSION, \
    HTTP_VARIABLE, POSTGRES_SETTINGS, LOGS JSON, KILL_API_JSON, OPTION INFO_JSON, OTHER INFO_TABLE, ID, \
    PROCESS_ID, ALLBESTMODELNAMEANDBRANCHID, SERVINGPIPELINETYPE, NAI DB, POSTGRES_CONFIG_SETTINGS, SASTOKEN,USE_RBAC_AUTH,AZURE_KEY_VAULT,TRUE,POSTGRES_PASS_KEY_DEV, AZURE_VAULT_KEYS, POSTGRES_PASS

from main.utils import linkedlist_traverse, logger_path, upload, update_pipeline graph
from main.utils.JsonParser import CreateNodeStructure
from main.utils.MainController import MainControllerRunner
from main.utils.link_list import LinkedList
import logging
import os
from fastapi import FastAPI, Request, Depends
from typing import Annotated
from datetime import datetime
import nltk
import aiofiles
from main.commons.constants_main import PIPELINE GRAPH, BISECT KMEAN,K MEANS,GMM, DATA_PROFILING SETTINGS, MODELALGOSETTINGS,DATASPLITSETTINGS, CLUSTERSETTINGS, DROOLSSETTINGS, VD_GBF_SETTINGS, STATE_KEY, DEAD_VALUE, MESSAGE_KEY, SUCCESS_VALUE, MESSAGE_VALUE, DEBUG_LOGGING
from main.components.modelling_algorithm.commons.commons constants model algo import DBSCAN,HDBSCAN_
from main.postgres_connection import PostgresConnector
import json
from main.components.source.utils.common_functions import dump_kill_api_data_postgresql, update_kill_api_details_in_postgresql, close_postgresql_connection, close_log_handlers
from AllCustomFunctions import CustomDataframeMapper
from main.components.source.commons.commons constants source import WHERE, QUERY
from main.components.segmentation.commons.commons_constants_segment import SEG CONTINOUS_CONDITION
from security services.azure key vault service.key vault manager import KeyVaultSingleton
from security services.cryptography.decrypt import decrypt_value
import ast

app = FastAPI()

# Process pool - each pipeline runs in an isolated process; memory freed on process exit
# max_tasks_per_child=5 reuses the warm worker across 5 pipelines before recycling, avoiding
# the cold spawn+import overhead (5 min delay) on every request
_pipeline_pool = ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context('spawn'), max_tasks_per_child=5)
_pool_lock = threading.Lock()

def _create_pool():
    return ProcessPoolExecutor(max_workers=1, mp_context=multiprocessing.get_context('spawn'), max_tasks_per_child=5)

def _get_or_recreate_pool():
    global _pipeline_pool
    with _pool_lock:
        if _pipeline_pool._broken:
            print("ProcessPoolExecutor is broken - recreating pool")
            _pipeline_pool.shutdown(wait=False)
            _pipeline_pool = _create_pool()
        return _pipeline_pool

HEADERS = {
    "User-Agent": "Python API Sample",
    "Content-Type": "application/json"
}
json_parser_object=CreateNodeStructure()
upper_layer_object=MainControllerRunner()
Clustering_models = [BISECT_KMEAN,K_MEANS,GMM,DBSCAN_,HDBSCAN_]


async def parse_body(request: Request):
    data: bytes = await request.body()
    return data
    
def convert_boolean_nulls(data):
    if isinstance(data, dict):
        return {k: convert_boolean_nulls(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_boolean_nulls(item) for item in data]
    elif data == 'True':
        return "true"
    elif data == 'False':
        return "false"
    else:
        return data

use_vault = str(config_file.get(AZURE_KEY_VAULT, "")).lower() == TRUE
key_vault_obj=None
if use_vault:
    KeyVaultSingleton()


def pipline_path_exist(pipelines_log_folder):
    if os.path.exists(pipelines_log_folder):
        print("Pipeline log folder already exists")
    else:
        os.makedirs(pipelines_log_folder)


def run_pipeline(data, log_file):
    pipeline_graph = {}
    status_dict = {}
    list_of_nodes_executed = []
    log_object = None
    try:
        unique_key = data[PIPELINEID] + "_" + data[TIMESTAMP] + '_' + data[VERSION]
        print(unique_key)
        
        with open(log_file, "w"):
            print("log file with write permission")
            
        print("MAIN log_file name ===========>", log_file, datetime.now())

        log_object = logging.getLogger(unique_key)
        log_level = logging.DEBUG if config_file.get(DEBUG_LOGGING, False) else logging.INFO
        log_object.setLevel(log_level)
        handler = logging.FileHandler(log_file)
        handler.setLevel(log_level)
        log_object.addHandler(handler)
        log_object.info(f"Logger initialized with level: {'DEBUG' if log_level == logging.DEBUG else 'INFO'}")
        log_object.info("UIII JSONN\n")
        log_object.info(data)
        log_object.info('\n \n ---------------------------------------------------- \n \n ')

        process_id = os.getpid()
        print("MAIN logging_object ============> ", log_object)
        postgres_settings = data.get(POSTGRES_SETTINGS, config_file[POSTGRES_CONFIG_SETTINGS])

        postgres_connector = PostgresConnector()
        postgres_connector.update_credentials(postgres_settings)
        postgres_connector.create_all_connections_and_tables()
        postgres_connector.set_genai_db_settings(postgres_settings)

        kill_api_body = {PROCESS_ID: process_id, ID: unique_key}
        dump_kill_api_data_postgresql(postgres_connector, kill_api_body, data)

        is_sink_present = json_parser_object.is_sink_available(data)
        json_dict = json_parser_object.json_setting_to_map(data)
        ll = LinkedList().linked_list_creation(data, pipeline_graph, json_dict)

        RESPONSE_STATUS = 0
        print('\n \n ---------------------------------------------------- \n \n ')
        head_node = ll
        nodes_completed = [EMPTY_STRING]

        flag, global_dataframe, model_sequence, serving_seqeunce, sink_node = 0, {}, {}, [], head_node
        recursion_results = linkedlist_traverse.linked_list_traversal(
            head_node, json_dict, nodes_completed, log_object, flag,
            global_dataframe, model_sequence, serving_seqeunce, sink_node,
            Clustering_models, upper_layer_object, list_of_nodes_executed, status_dict, pipeline_graph
        )

        if recursion_results != 0:
            flag, global_dataframe, model_sequence, serving_seqeunce, sink_node = recursion_results
            if is_sink_present == True:
                linked_list_json = sink_node.json
                log_object.info("\n############################################# {} Started #############################################".format(linked_list_json[ALGONAME]))
                setting_url = upper_layer_object.executioner(linked_list_json, json_dict, global_dataframe)
                dictionary_pass = {LINKED_LIST_JSON: linked_list_json, JSON_DICT: json_dict, DATA: global_dataframe,
                                   MODEL_SEQUENCE: model_sequence, SERVING_SEQUENCE: serving_seqeunce,
                                   "log_object": log_object, "pipeline_graph": pipeline_graph}
                response = setting_url.object_creator(dictionary_pass)
                status_dict.update(response[STATUS_DICT])
                list_of_nodes_executed.extend(response[LIST_OF_NODES_EXECUTED])
                pipeline_graph = response[PIPELINE_GRAPH]
                RESPONSE_STATUS = response[STATUS_CODE]
                log_object.info("############################################# {} Completed #############################################\n\n".format(linked_list_json[ALGONAME]))
            else:
                log_object.info("SINK NODE IS NOT PRESENT")
                RESPONSE_STATUS = 200
        else:
            flag = 500
            RESPONSE_STATUS = flag

        print("API RESPONSE STATUS CODE : ", flag)
        log_object.info("############################################# ALL PIPELINE NODES EXECUTED SUCCESSFULLY #############################################")

        if len(pipeline_graph) != len(list_of_nodes_executed):
            update_pipeline_graph.remove_unvisited_nodes(pipeline_graph, list_of_nodes_executed)
        upload.upload_error_logs(data, log_file, log_object)
        upload.upload_tracking_status(pipeline_graph, status_dict, list_of_nodes_executed, data, log_object)
        handlers = log_object.handlers[:]
        for handler in handlers:
            log_object.removeHandler(handler)
            handler.close()

        status_of_pipeline = {
            200: {STATE_KEY: SUCESS_VALUE, MESSAGE_KEY: EMPTY_STRING},
            500: {STATE_KEY: DEAD_VALUE, MESSAGE_KEY: MESSAGE_VALUE}
        }

        other_info_connector_obj = postgres_connector.get_nai_db_connection()
        filter_params = {PIPELINEID: data[PIPELINEID], VERSION: data[VERSION], TIMESTAMP: data[TIMESTAMP]}
        postgres_connector.update_column_in_postgresql(other_info_connector_obj, OTHER_INFO_TABLE, filter_params, KILL_API_JSON, "{}")
        postgres_connector.close_all_connections()
        print("MAIN log_file name ============> ", log_file, datetime.now())
        return status_of_pipeline[RESPONSE_STATUS]

    except Exception as ex:
        if log_object:
            log_object.info("Main API - Inside Exception block")
            log_object.error(f"ERROR INSIDE :: {data[PIPELINEID]} + '_' + data[TIMESTAMP] + '_' + data[VERSION]}", exc_info=True)
            log_object.info("\n")
        update_kill_api_details_in_postgresql(data)
        if len(pipeline_graph) != len(list_of_nodes_executed):
            update_pipeline_graph.remove_unvisited_nodes(pipeline_graph, list_of_nodes_executed)
        upload.upload_tracking_status(pipeline_graph, status_dict, list_of_nodes_executed, data, log_object)
        print("log_file : ", log_file)
        upload.upload_error_logs(data, log_file, log_object)
        close_log_handlers(log_object)
        close_postgresql_connection()
        print("Exception is --> ", ex)
        return {STATE_KEY: DEAD_VALUE, MESSAGE_KEY: MESSAGE_VALUE}


@app.post("/ss/json")
async def create(data: Annotated[bytes, Depends(parse_body)]):
    try:
        print("dev data is ---->", data)
        pipelines_log_folder = logger_path.logging_folder
        pipline_path_exist(pipelines_log_folder)

        json_str = data.decode().replace("true", "True").replace("false", "False")
        data = ast.literal_eval(json_str)
        data = convert_boolean_nulls(data)

        log_file = pipelines_log_folder + "/" + data[PIPELINEID] + "_" + data[TIMESTAMP] + "_" + data[VERSION] + ".log"

        # Submit pipeline to process pool - subprocess exits after completion, freeing all memory
        loop = asyncio.get_running_loop()

        pool = _get_or_recreate_pool()
        try:
            result = await loop.run_in_executor(pool, run_pipeline, data, log_file)
        except BrokenProcessPool:
            # Subprocess was killed (via kill API) or crashed - recreate pool for future
            # requests but do NOT retry, as that would undo the kill.
            print("BrokenProcessPool detected - recreating pool")
            _get_or_recreate_pool()
            result = {STATE_KEY: DEAD_VALUE, MESSAGE_KEY: MESSAGE_VALUE}
        return result

    except Exception as ex:
        print("Exception is --> ", ex)
        return {STATE_KEY: DEAD_VALUE, MESSAGE_KEY: MESSAGE_VALUE}

