from main.commons import constants_main
from main.components.sink.utils.commonFunctions import get_scheduling_settings
from main.commons.common_constants import SINK_ALGO_NAME, SINK_SAS_TOKEN, SASTOKEN, STORAGEACCOUNT, CONTAINERNAME, ESTYPE, \
    ALGONAME, ALGOTYPE, PIPELINEID, SETTINGS_JSON_KEY, TIMESTAMP, USERID, VERSION, \
    NODE_ID, EMPTY_STRING, GENAI, GENAI_SETTINGS, POSTGRES_SETTINGS, TRUE_KEY
from main.commons.constants_main import ACTIVE_DIRECTORY, OUTPUT_STORAGE_DIRECTORY_SINK, ALGO, AUTO_DEX_TABLE_HEADER_DATA, DATA_SOURCE, DROOLS_USER_ID, E

class CreateNodeStructure:

    def __init__(self):
        self.NODEID = NODE_ID
        
        self.seq_of_setting = [
            constants_main.SOURCE_SETTINGS_JSON_PROPERTY,
            constants_main.DATAPROFILING_SETTINGS_JSON_PROPERTY,
            constants_main.ETL_SETTINGS,
            constants_main.DROOLS_SETTINGS_JSON_PROPERTY,
            constants_main.DATA_SPLIT_SETTINGS,
            constants_main.MODEL_ALGO_SETTINGS_JSON_PROPERTY,
            constants_main.SINK_SETTINGS_JSON_PROPERTY,
            constants_main.VARIABLE_DERIVATION_SETTINGS_JSON_PROPERTY,
            constants_main.DATAVALIDATION_SETTINGS_JSON_PROPERTY,
            constants_main.AUTO_DEX_JSON_PROPERTY,
            constants_main.FEATURE_SETTINGS,
            constants_main.DATA_TRANSFORMATION_SETTINGS,
            GENAI_SETTINGS]
        self.evaluator = constants_main.EVALUATOR_SETTINGS
        self.scheduling_settings = constants_main.SCHEDULING_SETTINGS_KEY
        self.dex_action = constants_main.DEX_IN_ACTION_SETTINGS
        self.node_sequence=constants_main.LINKED_NODE_OF_NAI_COMPONENTS
        
        self.same_node_for_different_func = [
            constants_main.DATAEXPLORATION_SETTINGS_JSON_PROPERTY,
            constants_main.DATAEXPLORATION_SETTINGS_JSON_PROPERTY_TS,
            constants_main.SEGMENTATION_CREATION_SETTINGS_JSON_PROPERTY,
            constants_main.GROUPBY_ALGO_SETTINGS_JSON_PROPERTY,
            constants_main.CROSS_TAB_FUNCTIONALITY_SETTINGS_JSON_PROPERTY]
            
        self.only_settings = [constants_main.JUPYTER_SETTINGS]
        self.time_stamp = TIMESTAMP
        self.pipeline_id = PIPELINEID
        self.version = VERSION
        self.userid = USERID
        self.train_valid = constants_main.TRAIN_VALIDATION
        self.train_data_count = constants_main.TRAIN_DATA_COUNT_FLAG
        self.vd_columns = constants_main.INTERMEDIATE_VD_COLUMNS
        self.intermediate_vds = constants_main.INTERMEDIATE_VD_SUBMIT
        self.vd_process_id = constants_main.VD_PROCESS_ID
        self.index_and_type = constants_main.AUTODEX_INDEX
        self.model_save_on_whole_data_set_flag = constants_main.MODEL_SAVE_ON_WHOLE_DATA_SET_FLAG
        self.scheduling = constants_main.SCHEDULING_TAG
        self.delete_existing_pipeline = constants_main.DELETE_EXISTING_PATH_TAG
        self.sanity_test = constants_main.SANITY_TEST_TAG
        self.categorical_columns = constants_main.CATEGORICAL_COLUMNS
        self.delete_existing_path = constants_main.DELETE_EXISTING_PATH_TAG
        self.categorical_columns_data = EMPTY_STRING
        self.localpath = constants_main.LOCAL_STORAGE_PATH
        self.intermediate_sink = constants_main.INTERMEDIATE_SINK
        self.postgres_settings = POSTGRES_SETTINGS
        
        self.common_sink_settings = {}
        self.sink_algo_name = ""

    def json_setting_to_map(self, json_val):
        
        #for affinity model and feature tool case we need to have sink info to dump their model
        self.common_sink_settings = self.sink_common_properties(json_val)
        
        final_output = {}
        for current_setting_key in self.seq_of_setting:
            try:
                print("current_setting_key--->", current_setting_key)
                json_segment_to_parse = json_val[current_setting_key]
                print("json_segment_to_parse---->", json_segment_to_parse)
                self.create_dict_seq_of_setting(json_segment_to_parse, json_val, final_output, current_setting_key)
            except Exception:
                pass
                
        for current_setting_key in self.same_node_for_different_func:
            try:
                if current_setting_key in [constants_main.DATAEXPLORATION_SETTINGS_JSON_PROPERTY, constants_main.DATAEXPLORATION_SETTINGS_JSON_PROPERTY_TS]:
                    self.create_dict_dex(json_val, final_output, current_setting_key)
                    
                else:
                    self.create_dict_same_node_diff_func(json_val, final_output, current_setting_key)
                    
            except Exception:
                pass
                
        for current_setting_key in self.only_settings:
            try:
                json_segment_to_parse = json_val[current_setting_key]
                id_ = json_segment_to_parse[self.NODEID]
                final_output[id_] = json_segment_to_parse
            except Exception:
                pass
                
        return final_output

    def create_dict_seq_of_setting(self, json_segment_to_parse, json_val, final_output, current_setting_key):
        if current_setting_key in ["sinkSettings", "dataTransformationSettings", "featureSettings"]:
            self.create_dict_sink_dt_feature(json_segment_to_parse, json_val, final_output, current_setting_key)
            
        elif current_setting_key == "dataValidationSettings":
            self.create_dict_data_validation(json_segment_to_parse, json_val, final_output)
            
        elif current_setting_key == "sourceSettings":
            self.create_dict_source(json_segment_to_parse, json_val, final_output)
            
        elif current_setting_key == "autoDexSettings":
            self.create_dict_auto_dex(json_segment_to_parse, json_val, final_output)

        elif current_setting_key == "genAiSettings":
            self.create_dict_genai(json_segment_to_parse, json_val, final_output)
        else:
            self.create_dict(json_segment_to_parse, json_val, final_output, current_setting_key)

    def create_dict_genai(self, json_segment_to_parse, json_val, final_output):
        for sequence in json_segment_to_parse:
            id_ = sequence[self.NODEID]
            sequence[self.time_stamp] = json_val[self.time_stamp]
            sequence[self.pipeline_id] = json_val[self.pipeline_id]
            sequence[self.version] = json_val[self.version]
            sequence[self.userid] = json_val[self.userid]
            final_output[id_] = {GENAI : sequence}

    def create_dict_sink_dt_feature(self, json_segment_to_parse, json_val, final_output, current_setting_key):
        #In case of feature tool we need label_column, to remove it from becoming feature in feature_tool.pickle
        label_column = ""
        for model_settings in json_val[constants_main.MODEL_ALGO_SETTINGS_JSON_PROPERTY]:
            if constants_main.LABEL_COLUMN in model_settings and model_settings[constants_main.LABEL_COLUMN] not in [None, ""]:
                label_column = model_settings[constants_main.LABEL_COLUMN]




        for sequence in json_segment_to_parse:
            sequence[self.time_stamp] = json_val[self.time_stamp]
            sequence[self.pipeline_id] = json_val[self.pipeline_id]
            sequence[self.version] = json_val[self.version]
            sequence[self.userid] = json_val[self.userid]
            sequence[ACTIVE_DIRECTORY] = json_val[ACTIVE_DIRECTORY]
            sequence[self.evaluator] = json_val[self.evaluator]
            sequence[self.train_valid] = json_val[self.train_valid]
            sequence[self.train_data_count] = json_val[self.train_data_count]
            sequence[self.model_save_on_whole_data_set_flag] = json_val[self.model_save_on_whole_data_set_flag]
            sequence[self.scheduling] = json_val[self.scheduling]
            sequence[self.scheduling_settings] = get_scheduling_settings() if sequence[self.scheduling] == TRUE_KEY else {}
            sequence[self.delete_existing_pipeline] = json_val[self.delete_existing_pipeline]
            sequence[self.sanity_test] = json_val[self.sanity_test]

            sequence[self.categorical_columns] = self.categorical_columns_data
            sequence[self.delete_existing_path] = json_val[self.delete_existing_path]
            sequence[self.intermediate_sink] = json_val[self.intermediate_sink]
            sequence[self.postgres_settings] = json_val.get(self.postgres_settings, None)
            sequence[constants_main.LABEL_COLUMN] = label_column
            
            #for auto dex case only
            try:
                sequence[self.index_and_type] = json_val[self.index_and_type]
            except Exception:
                pass
                
            try:
                sequence[self.categoricalColumns]=json_val[constants_main.SOURCE_SETTINGS_JSON_PROPERTY][constants_main.CATEGORICAL_COLUMNS]
            except Exception:
                pass
                
            try:
                sequence[self.localpath] = json_val[self.localpath]
            except Exception:
                pass
                
            id_ = sequence[self.NODEID]
            
            if current_setting_key == "featureSettings":
                sequence[constants_main.SINK_SETTINGS_JSON_PROPERTY] = self.common_sink_settings
            final_output[id_] = sequence

    def create_dict_data_validation(self, json_segment_to_parse, json_val, final_output):
        for sequence in json_segment_to_parse:
            sequence[self.time_stamp] = json_val[self.time_stamp]
            sequence[self.pipeline_id] = json_val[self.pipeline_id]
            sequence[self.version] = json_val[self.version]
            sequence[self.userid] = json_val[self.userid]
            sequence[ACTIVE_DIRECTORY]=json_val[ACTIVE_DIRECTORY]
            sequence[self.evaluator]=json_val[self.evaluator]

            sequence[self.train_valid] = json_val[self.train_valid]
            sequence[self.train_data_count] = json_val[self.train_data_count]
            sequence[self.categorical_columns] = self.categorical_columns_data
            sequence[self.postgres_settings] = json_val.get(self.postgres_settings, None)
            id_ = sequence[self.NODEID]
            final_output[id_] = sequence

    def create_dict_source(self, json_segment_to_parse, json_val, final_output):
        categorical_columns_list = []
        for sequence in json_segment_to_parse:
            id_ = sequence[self.NODEID]
            categorical_columns_list.append(sequence[self.categorical_columns])
            #FOR ADD VD API
            try:
                sequence[self.intermediate_vds] = json_val[self.intermediate_vds]
                sequence[self.vd_columns] = json_val[self.vd_columns]
                sequence[self.vd_process_id] = json_val[self.vd_process_id]
                sequence[self.vd_columns] = json_val[self.vd_columns]
                sequence[ESTYPE] = "vdintermediatetype"
                
            except Exception:
                pass
            final_output[id_] = sequence
            
        self.categorical_columns_data = ",".join(categorical_columns_list)

    def create_dict_auto_dex(self, json_segment_to_parse, json_val, final_output):
        for sequence in json_segment_to_parse:
            id_ = sequence[self.NODEID]
            sequence[self.time_stamp] = json_val[self.time_stamp]
            sequence[self.pipeline_id] = json_val[self.pipeline_id]
            sequence[self.version] = json_val[self.version]
            sequence[self.userid] = json_val[self.userid]
            sequence[constants_main.DEX_IN_ACTION_SETTINGS] = json_val[constants_main.DEX_IN_ACTION_SETTINGS] if len(json_val[constants_main.DEX_IN_ACTION_SETTINGS]) > 0 else None
            final_output[id_] = sequence

    def create_dict(self, json_segment_to_parse, json_val, final_output, current_setting_key):
        #for case of lir, we need label column information so that we donot standard scaler on it
        # incase of cluster no label column info so issue with train test split for equilabel distribution
        label_column = ""
        for model_settings in json_val[constants_main.MODEL_ALGO_SETTINGS_JSON_PROPERTY]:
            model_settings[self.categorical_columns] = self.categorical_columns_data
            try:
                model_settings[self.categoricalColumns]=json_val[constants_main.SOURCE_SETTINGS_JSON_PROPERTY][constants_main.CATEGORICAL_COLUMNS]
            except Exception:
                pass
            print(model_settings)
            if constants_main.LABEL_COLUMN in model_settings and model_settings[constants_main.LABEL_COLUMN] not in [None, ""]:
                label_column = model_settings[constants_main.LABEL_COLUMN]
                
        for nodes in json_segment_to_parse:
            nodes[self.sanity_test]=json_val[self.sanity_test]
            nodes[constants_main.LABEL_COLUMN] = label_column
            nodes[self.time_stamp] = json_val[self.time_stamp]
            nodes[self.pipeline_id] = json_val[self.pipeline_id]
            nodes[self.version] = json_val[self.version]
            nodes[self.userid] = json_val[self.userid]
            nodes[ACTIVE_DIRECTORY]=json_val[ACTIVE_DIRECTORY]
            nodes[self.categorical_columns] = self.categorical_columns_data
            nodes[constants_main.SINK_SETTINGS_JSON_PROPERTY] = self.common_sink_settings
            try:
                id_ = nodes[self.NODEID]
            except Exception:
                if current_setting_key == constants_main.DROOLS_SETTINGS_JSON_PROPERTY:
                    nodes[self.NODEID]='-1'
                    id_ = '-1'
            final_output[id_] = nodes

    def create_dict_dex(self, json_val, final_output, current_setting_key):
        json_segment_to_parse=json_val[current_setting_key]
        id_=json_segment_to_parse[0][self.NODEID]
        for sequence in json_segment_to_parse:
            sequence[self.time_stamp] = json_val[self.time_stamp]
            sequence[self.pipeline_id] = json_val[self.pipeline_id]
            sequence[self.version] = json_val[self.version]
            sequence[self.userid] = json_val[self.userid]
            sequence[self.categorical_columns] = self.categorical_columns_data
            sequence[constants_main.DEX_IN_ACTION_SETTINGS] = json_val[constants_main.DEX_IN_ACTION_SETTINGS] if len(json_val[constants_main.DEX_IN_ACTION_SETTINGS]) > 0 else {}
        final_output[id_] = json_segment_to_parse

    def create_dict_same_node_diff_func(self, json_val, final_output, current_setting_key):
        json_segment_to_parse=json_val[current_setting_key]
        id_=json_segment_to_parse[0][self.NODEID]
        for sequence in json_segment_to_parse:
            sequence[self.time_stamp] = json_val[self.time_stamp]
            sequence[self.pipeline_id] = json_val[self.pipeline_id]
            sequence[self.version] = json_val[self.version]
            sequence[self.userid] = json_val[self.userid]
        final_output[id_] = json_segment_to_parse

    # Creates a node for an operation in the pipeline, which is used to keep track of the flow of execution of the pipeline.
    def create_graph_node(self, final_dict, current_id, pipeline_graph, next_nodes):
        print('\n\n')
        print(final_dict[current_id], '------', next_nodes)
        list_of_key = self.create_key(final_dict[current_id], current_id)
        list_of_next_nodes = []
        for id in next_nodes.split(','):
            if id!="" and id in final_dict:
                list_of_next_nodes = list_of_next_nodes + (self.create_key(final_dict[id], id))
                
        for key in list_of_key:
            pipeline_graph[key] = list_of_next_nodes


    # Creates the key name for the node i.e. the combination of type of operation and node_id.
    def create_key(self, final_dict, current_id):
        
        if(SOURCE_CATEGORY in final_dict):
            return ['Source_'+final_dict[DATA_SOURCE]+'_'+current_id]
        elif(OUTPUT_STORAGE_DIRECTORY in final_dict or "esIndex" in final_dict or "esUser" in final_dict):
            return ['Sink_'+final_dict[SINK_TYPE]+'_'+current_id]
        elif(ALGONAME in final_dict):
            return [final_dict[ALGONAME]+'_'+current_id]
        elif(ALGOTYPE in final_dict):
            return [final_dict[ALGOTYPE]+'_'+current_id]
        elif(ALGO in final_dict):
            return [final_dict[ALGO] + '_' + current_id]
        elif(AUTO_DEX_TABLE_HEADER_DATA in final_dict):
            return ['AutoDex_'+current_id]
        elif(DROOLS_USER_ID in final_dict):
            return ['Drool_'+ current_id]
        elif(FEATURE_TOOL in final_dict):
            return ['FeatureTool_' + current_id]
        elif(SINGLE in final_dict):
            return self.create_key_for_data_validation(final_dict)
        elif(GENAI in final_dict):
            return [GENAI+"_"+current_id]
        else:
            return self.create_key_for_same_node_diff_operation(final_dict, current_id)

    def create_key_for_data_validation(self, final_dict):
        current_id = final_dict[NODE_ID]
        list_of_keys = []
        for case in final_dict[SINGLE]:
            list_of_keys = list_of_keys + self.create_key_for_multiple_algo_name_same_operation(case[VALIDATION_ALGO_NAMES], current_id)
        for case in final_dict[MULTIPLE]:
            list_of_keys = list_of_keys + self.create_key_for_multiple_algo_name_same_operation(case[VALIDATION_ALGO_NAMES], current_id)
        return list_of_keys

    def create_key_for_same_node_diff_operation(self, final_seq, current_id):
        
        list_of_keys = []
        if(EXPLORATION_ALGO_NAMES in final_seq[0]):
            for dict in final_seq:
                list_of_keys= list_of_keys + self.create_key_for_multiple_algo_name_same_operation(dict[EXPLORATION_ALGO_NAMES], current_id)
                
        elif (ALGONAME in final_seq[0]):
            list_of_keys.append(final_seq[0][ALGONAME] + "_" + current_id)
        return list_of_keys

    def create_key_for_multiple_algo_name_same_operation(self, algo_names, current_id):
        list_of_keys = []
        list_of_algonames = algo_names.split(',')
        for algo_name in list_of_algonames:
            x = algo_name + '_' + str(current_id)
            list_of_keys.append(x)
            
        return list_of_keys

    def is_sink_available(self, json_val):
        flag=False
        for nodes in json_val[self.node_sequence]:
            if nodes[SETTINGS_JSON_KEY] == "sinkSettings":
                self.sink_algo_name = nodes.get("algoName", "")
                flag=True
        return flag


    def sink_common_properties(self, json_val):
        lst = json_val.get(constants_main.SINK_SETTINGS_JSON_PROPERTY, [])
        settings = lst[0].copy() if isinstance(lst, list) and lst else {}
        settings[constants_main.LOCAL_STORAGE_PATH] = json_val.get(constants_main.LOCAL_STORAGE_PATH, "")
        settings[ACTIVE_DIRECTORY] = json_val.get(ACTIVE_DIRECTORY, "")
        settings["sinkAlgoName"] = self.sink_algo_name
        settings[self.time_stamp] = json_val.get(self.time_stamp)
        settings[self.pipeline_id] = json_val.get(self.pipeline_id)
        settings[self.version] = json_val.get(self.version)
        settings[self.userid] = json_val.get(self.userid)
        return settings