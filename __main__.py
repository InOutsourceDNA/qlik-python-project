
import argparse
import json
import logging
import logging.config
import os
import sys
import time
import re
import itertools
from concurrent import futures
from datetime import datetime
import pandas as pd
#from mlxtend.frequent_patterns import apriori
#from mlxtend.frequent_patterns import association_rules
from elasticsearch import Elasticsearch
from fuzzywuzzy import fuzz
import Levenshtein
# Add Generated folder to module path.
PARENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(PARENT_DIR, 'generated'))

import ServerSideExtension_pb2 as SSE
import grpc
from ssedata import FunctionType
from scripteval import ScriptEval



_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class ExtensionService(SSE.ConnectorServicer):
    """
    A simple SSE-plugin created for the HelloWorld example.
    """

    def __init__(self, funcdef_file):
        """
        Class initializer.
        :param funcdef_file: a function definition JSON file
        """
        self._function_definitions = funcdef_file
        self.ScriptEval = ScriptEval()
        os.makedirs('logs', exist_ok=True)
        log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logger.config')
        logging.config.fileConfig(log_file)
        logging.info('Logging enabled')

    @property
    def function_definitions(self):
        """
        :return: json file with function definitions
        """
        return self._function_definitions

    @property
    def functions(self):
        """
        :return: Mapping of function id and implementation
        """
        return {
            0: '_defendantMatch' #ADD YOUR FUNCTION HERE
        }



    @staticmethod
    def _get_function_id(context):
        """
        Retrieve function id from header.
        :param context: context
        :return: function id
        """
        metadata = dict(context.invocation_metadata())
        header = SSE.FunctionRequestHeader()
        header.ParseFromString(metadata['qlik-functionrequestheader-bin'])

        return header.functionId

    """
    Implementation of added functions.
    """

    @staticmethod
    def _defendantMatch(request, context):
        """
        Mirrors the input and sends back the same data.
        :param request: iterable sequence of bundled rows
        :return: the same iterable sequence as received
        """
        defendantList = []
        uniqueidlist = []
        #productIdList = []
        #purchasedList = []
        for request_rows in request:
            #print(request_rows)
            for row in request_rows.rows:
                # the first numData contains the orderIds
                defendantList.append([d.strData for d in row.duals][0])
                uniqueidlist.append([d.strData for d in row.duals][1])
        # for defendant in defendantList:
        #     myResult = term_vector_query(defendant)
            
            # Get termvectors query result for current defendant
            # Find best ngram
            # do other stuf
            # Get result of match_phrase query
            # Finalize ClientName to be returned
            # Add final clientname to final list to be returned to Qlik
            
        # Create an iterable of dual with the result
        # CONSTANTS
        UNIQUE_ID_COL = 'AlertUniqueID'
        FINAL_CLIENT_NAME_COL = 'ClientNameFinal'
        FINAL_CLIENT_NUM_COL = 'ClientNumberFinal'
        PROCESSED_CLIENT_NAME_COL = 'ClientNameProcessed'
        TOKEN_SORT_RATIO_COL = 'TokenSortFinal'
        TOKEN_SET_RATIO_COL = 'TokenSetRatioFinal'
        PARTIAL_RATIO_COL = 'PartialRatioFinal'
        LEV_RATIO_COL = 'LevenshteinRatioFinal'
        AVERAGE_RATIO_COL = 'AverageRatio'
        
        N_GRAM = 'ngram'
        
        es = Elasticsearch(['10.0.1.224:9201'])
        
        # stoplist = stopwords.words('english')
        stoplist = ["-", "'", ",", ".", "&", "the", "inc.", "llc", "/b/"]
        
        threshold = 70
        
        
        def term_vector_query(input_search_name):
            term_data = {
                "doc": {"clientname1": input_search_term},
                "fields": ["clientname1.normal", "clientname1.2gram", "clientname1.3gram"],
                "term_statistics": True,
                "field_statistics": False,
                "positions": False,
                "offsets": False,
                "filter": {
                    "max_num_terms": 3,
                    "min_term_freq": 1,
                    "min_doc_freq": 1,
                    "max_doc_freq": 10
                }
            }
            r = es.termvectors(index='elasticvectors3', doc_type='_doc', body=term_data)
            return r
        
        
        def search_query(search_data):
            r = es.search(index='elasticvectors3', doc_type='_doc', body={
                "query": {
                    "match_phrase": {
                        "clientname1.normal": {
                            "query": search_data
                        }
                    }
                }
            })
            return r
        
        
        def generate_data_row(final_client_name=None, token_set_ratio=None, token_sort_ratio=None, lev_ratio=None, final_client_number=None, average_ratio=None):
            return {               
                FINAL_CLIENT_NAME_COL: final_client_name,
                FINAL_CLIENT_NUM_COL: final_client_number,
                TOKEN_SET_RATIO_COL: token_set_ratio,
                TOKEN_SORT_RATIO_COL: token_sort_ratio,
                LEV_RATIO_COL: lev_ratio,
                AVERAGE_RATIO_COL: average_ratio
                
            }
        
        
        def get_highest_ngram(search_results):
            # search_results looks like: [('DOCUMENT NAME', 'clientname2.ngram', 96), ...]
            filtered_results = []
        
            # Example: key could be "clientname1.2gram" and terms would be "terms": {"allstate insurance": {...}}
            highest_ngram = 'normal'
            for result in search_results:
                if result[N_GRAM].endswith('3gram'):
                    highest_ngram = '3gram'
                    break
                elif result[N_GRAM].endswith('2gram'):
                    highest_ngram = '2gram'
        
            for result in search_results:
                if result[N_GRAM].endswith(highest_ngram):
                    filtered_results.append(result)
        
            return filtered_results
        
        
        def get_best_result(search_results):
            highest_score = 0
            best_result = None
        
            for result in search_results:
                if result[TOKEN_SET_RATIO_COL] > highest_score:
                    highest_score = result[TOKEN_SET_RATIO_COL]
                    best_result = result
            return best_result
        
        
        # def levenshteinDistance(s1, s2):
        #     if len(s1) > len(s2):
        #         s1, s2 = s2, s1
        
        #     distances = range(len(s1) + 1)
        #     for i2, c2 in enumerate(s2):
        #         distances_ = [i2 + 1]
        #         for i1, c1 in enumerate(s1):
        #             if c1 == c2:
        #                 distances_.append(distances[i1])
        #             else:
        #                 distances_.append(1 + min((distances[i1], distances[i1 + 1], distances_[-1])))
        #         distances = distances_
        #     return distances[-1]
        
        
        
        def fuzzy_search(str1, str2):
            ratio = Levenshtein.ratio(str1, str2)*100
            partial_ratio = fuzz.partial_ratio(str1, str2)
            token_sort_ratio = fuzz.token_sort_ratio(str1, str2)
            token_set_ratio = fuzz.token_set_ratio(str1, str2)
            return ratio, partial_ratio, token_sort_ratio, token_set_ratio
        
        
        # modify this function to optimize for speed
        def remove_stopwords(string):
            # tokens = word_tokenize(string.lower())
            tokens = string.lower().split()
            # stopwords = ["the",",",".","'",]
            # tokens_without_sw = [word for word in tokens if not word in stopwords]
            tokens_without_sw = [word for word in tokens if not word in stoplist]
            # Alternative Syntax:
            # tokens_without_sw = []
            # for word in tokens:
            #  if word not in stopwords.words():
            #    tokens_without_sw.append(word)
            # tokens_without_punc = [word for word in tokens_without_sw if word.isalnum()]
        
            string_without_sw = ' '.join(tokens_without_sw)
            return ''.join(e for e in string_without_sw if e.isalnum() or e == ' ')
        
        
        #df = pd.read_excel('NCFI_practice.xlsx', sheet_name='Sheet1', usecols='B:C')
       
        df = pd.DataFrame({'SearchedName': defendantList, 'AlertUniqueID': uniqueidlist}, columns=['SearchedName', 'AlertUniqueID'])
        
        
        
        
    
        logging.info('Rows in dataframe: ' + str(len(df.index)))
                     
        df['SearchedName'] = df['SearchedName'].apply(remove_stopwords)
        
        data_rows = []
        
        manual_override = {
            "lincoln national life insurance company": "Lincoln Financial",
            "the lincoln national life insurance company": "Lincoln Financial"  
        }
        # pandas convert csv file into dictionary with todic
        
                      
        with open ('first-names.txt') as myfile: 
            human_names = [line.rstrip().lower() for line in myfile]
                
        
        
        client_name_alias_map = {
            "walmart": ['wal-mart']
        }
        
        
        #df = df.to_frame()   
        
        
        # Iterate over every excel row
        for index, row in df.iterrows():
            input_search_name = row['SearchedName'].lower()
            
            #unique_id = row['AlertUniqueID']
            # HANDLE MANUAL OVERRIDE LOGIC
            if input_search_name in manual_override:
                data_rows.append(generate_data_row(final_client_name=manual_override[input_search_name]))
                continue
            
            for name in human_names:
                if name in input_search_name and len(input_search_name.split()) > 3:
                    data_rows.append(generate_data_row())
                    continue
                    
            # HANDLE ALIAS LOGIC
            input_search_terms = []
            for key, alias_list in client_name_alias_map.items():
                if key in input_search_name:
                    for alias in alias_list:
                        input_search_terms.append(input_search_name.replace(key, alias).lower())
            # If input_search_terms is empty, it means there were no aliases-- instead just add the original term to the list
            if not input_search_terms:
                input_search_terms.append(input_search_name)
        
            # PERFORM TERM VECTOR QUERIES
            all_term_vector_results = []
            for input_search_term in input_search_terms:
                # Note: For some reason ES throws an exception when no term vectors exist for the query, so we need to
                # wrap the query in a try/catch block
                try:
                    # Get the query result
                    term_vector_search_result = term_vector_query(input_search_term)['term_vectors']
                    all_term_vector_results.append(term_vector_search_result)
                except:
                    data_rows.append(generate_data_row())
                    continue
        
            # PERFORM SEARCH QUERIES
            all_search_results = []
            for term_vector_result in all_term_vector_results:
                # Iterate over every term in term vector query
                for n_gram, terms in term_vector_result.items():
                    for term, term_data in terms['terms'].items():
                        if "doc_freq" not in term_data:
                            continue
                        # if int(term_data["doc_freq"]) > 10:
                        #    continue
                        search_result = search_query(term)
                        output_search_result = []
                        # For every term, do a search query
                      #  
                        for hits in search_result['hits']['hits']:
                            output_client_name = hits['_source']['clientname1']
                            output_client_number = hits['_source']['clientnumber']
                            
                            # Compare the search query result to the SearchedName
                            lev_ratio, partial_ratio, token_sort_ratio, token_set_ratio = fuzzy_search(
                                input_search_name.lower(),
                                output_client_name.lower()
                            )
                            
                            average_ratio = ((token_set_ratio+token_sort_ratio+lev_ratio)/3)
                            
                            output_client_name_processed = remove_stopwords(output_client_name.lower())
                            # Keep track of all search query results FOR THIS TERM in a list
                            output_search_result.append({
                                FINAL_CLIENT_NAME_COL: output_client_name,
                                N_GRAM: n_gram,
                                TOKEN_SET_RATIO_COL: token_set_ratio,
                                TOKEN_SORT_RATIO_COL: token_sort_ratio,
                                LEV_RATIO_COL: lev_ratio,
                                PROCESSED_CLIENT_NAME_COL: output_client_name_processed,
                                FINAL_CLIENT_NUM_COL: output_client_number,
                                AVERAGE_RATIO_COL: average_ratio
                            })
                        # Keep track of ALL search query results (across all terms) in a list
                        all_search_results.extend(output_search_result)
          #  import pdb;pdb.set_trace() 
            ##implement logic for matching Proper Noun and Selecting highest N_Gram
            if not all_search_results:
                data_rows.append(generate_data_row())
                continue
        
            ##WRITE nltk function: with example and run that
            filtered_results = []
            input_search_name_first_word = input_search_name.split()[0].lower()
        
            found = False
            # Look for the first word match
            for result in all_search_results:
                document_name = result[PROCESSED_CLIENT_NAME_COL]
        
                # E.g. Allstate Insurance Company => ['Allstate', 'Insurance', 'Company']
                if input_search_name_first_word == document_name.split()[0]:
                    found = True
        
            if found:
                for idx, result in enumerate(all_search_results):
                    document_name = result[PROCESSED_CLIENT_NAME_COL]
        
                    # E.g. Allstate Insurance Company => ['Allstate', 'Insurance', 'Company']
                    if input_search_name_first_word == document_name.split()[0]:
                        filtered_results.append(result)
            else:
                data_rows.append(generate_data_row())
                continue
        
            filtered_results = get_highest_ngram(filtered_results)
            best_result = get_best_result(filtered_results)  
            if best_result[AVERAGE_RATIO_COL] < threshold:
                data_rows.append(
                    generate_data_row(
                        token_set_ratio=best_result[TOKEN_SET_RATIO_COL],
                        token_sort_ratio=best_result[TOKEN_SORT_RATIO_COL],
                        lev_ratio=best_result[LEV_RATIO_COL],
                        average_ratio=best_result[AVERAGE_RATIO_COL]
                    )
                )
                continue
            #import pdb;pdb.set_trace() 
            data_rows.append(
                generate_data_row(
                    final_client_name=best_result[FINAL_CLIENT_NAME_COL],
                    token_set_ratio=best_result[TOKEN_SET_RATIO_COL],
                    token_sort_ratio=best_result[TOKEN_SORT_RATIO_COL],
                    lev_ratio=best_result[LEV_RATIO_COL],
                    final_client_number=best_result[FINAL_CLIENT_NUM_COL],
                    average_ratio=best_result[AVERAGE_RATIO_COL]
                )
            )
        
        result_df = pd.DataFrame.from_records(data_rows)
        for col in result_df:
            df[col] = result_df[col]
                   
                   
        #df['AverageRatio'] = df[[LEV_RATIO_COL, TOKEN_SORT_RATIO_COL, TOKEN_SET_RATIO_COL]].mean(axis=1)
        
        final_df =  df[["SearchedName","AlertUniqueID", "ClientNameFinal", "ClientNumberFinal", "AverageRatio"]] 
        
        final_df["AlertUniqueID"] = final_df["AlertUniqueID"].astype(str)
        final_df["ClientNumberFinal"] = final_df["ClientNumberFinal"].astype(str)
        final_df["AverageRatio"] = final_df["AverageRatio"].astype(str)
        
        
        final_listA = final_df["SearchedName"].values.tolist()
        final_listB = final_df["AlertUniqueID"].values.tolist()
        final_listC = final_df["ClientNameFinal"].values.tolist()
        final_listD = final_df["ClientNumberFinal"].values.tolist()
        final_listE = final_df["AverageRatio"].values.tolist()
        
        
          # Create an iterable of dual with the result
        dualsList = []
        dualsList.append([SSE.Dual(strData=d) for d in final_listA])
        dualsList.append([SSE.Dual(strData=d) for d in final_listB])
        dualsList.append([SSE.Dual(strData=d) for d in final_listC])
        dualsList.append([SSE.Dual(strData=d) for d in final_listD])
        dualsList.append([SSE.Dual(strData=d) for d in final_listE])
                         
        response_rows = []
        for i in range(len(final_listA)):
            duals = [dualsList[z][i] for z in range(len(dualsList))]
            response_rows.append(SSE.Row(duals=iter(duals)))

        #yield SSE.BundledRows(rows=dualsList)
        #yield SSE.BundledRows(rows=[SSE.Row(duals=duals)])
        yield SSE.BundledRows(rows=response_rows)

        # duals = iter([[SSE.Dual(strData=d)] for d in final_list])
        # # Yield the row data as bundled rows
        # yield SSE.BundledRows(rows=[SSE.Row(duals=d) for d in duals])
        
        
        # response_rows = []
        # for i in range(len(defendantList)):
        #     duals = [dualsList[z][i] for z in range(len(dualsList))]
        #     response_rows.append(SSE.Row(duals=iter(duals)))
       
        
        # table = SSE.TableDescription(name='Defendants')
        # table.fields.add(dataType=SSE.STRING)
        # table.fields.add(dataType=SSE.STRING)
        # md = (('qlik-tabledescription-bin', table.SerializeToString()),)
        # context.send_initial_metadata(md)

        # yield SSE.BundledRows(rows=response_rows)
        # duals = iter([[SSE.Dual(strData=d)] for d in defendantList])
        # duals = iter([[SSE.Dual(strData=d)] for d in final_df['ClientNameFinal']])
        # # Yield the row data as bundled rows
        # yield SSE.BundledRows(rows=[SSE.Row(duals=d) for d in duals])

        
    """
    Implementation of rpc functions.
    """

    def GetCapabilities(self, request, context):
        """
        Get capabilities.
        Note that either request or context is used in the implementation of this method, but still added as
        parameters. The reason is that gRPC always sends both when making a function call and therefore we must include
        them to avoid error messages regarding too many parameters provided from the client.
        :param request: the request, not used in this method.
        :param context: the context, not used in this method.
        :return: the capabilities.
        """
        logging.info('GetCapabilities')
        # Create an instance of the Capabilities grpc message
        # Enable(or disable) script evaluation
        # Set values for pluginIdentifier and pluginVersion
        capabilities = SSE.Capabilities(allowScript=True,
                                        pluginIdentifier='Sentiment',
                                        pluginVersion='v1.1.0')

        # If user defined functions supported, add the definitions to the message
        with open(self.function_definitions) as json_file:
            # Iterate over each function definition and add data to the capabilities grpc message
            for definition in json.load(json_file)['Functions']:
                function = capabilities.functions.add()
                function.name = definition['Name']
                function.functionId = definition['Id']
                function.functionType = definition['Type']
                function.returnType = definition['ReturnType']

                # Retrieve name and type of each parameter
                for param_name, param_type in sorted(definition['Params'].items()):
                    function.params.add(name=param_name, dataType=param_type)

                logging.info('Adding to capabilities: {}({})'.format(function.name,
                                                                     [p.name for p in function.params]))

        return capabilities

    def ExecuteFunction(self, request_iterator, context):
        """
        Execute function call.
        :param request_iterator: an iterable sequence of Row.
        :param context: the context.
        :return: an iterable sequence of Row.
        """
        # Retrieve function id
        func_id = self._get_function_id(context)

        # Call corresponding function
        logging.info('ExecuteFunction (functionId: {})'.format(func_id))

        return getattr(self, self.functions[func_id])(request_iterator, context)

    def EvaluateScript(self, request, context):
        """
        This plugin provides functionality only for script calls with no parameters and tensor script calls.
        :param request:
        :param context:
        :return:
        """
        # Parse header for script request
        metadata = dict(context.invocation_metadata())
        header = SSE.ScriptRequestHeader()
        header.ParseFromString(metadata['qlik-scriptrequestheader-bin'])

        # Retrieve function type
        func_type = self.ScriptEval.get_func_type(header)

        # Verify function type
        if (func_type == FunctionType.Aggregation) or (func_type == FunctionType.Tensor):
            return self.ScriptEval.EvaluateScript(header, request, context, func_type)
        else:
            # This plugin does not support other function types than aggregation  and tensor.
            # Make sure the error handling, including logging, works as intended in the client
            msg = 'Function type {} is not supported in this plugin.'.format(func_type.name)
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details(msg)
            # Raise error on the plugin-side
            raise grpc.RpcError(grpc.StatusCode.UNIMPLEMENTED, msg)

    """
    Implementation of the Server connecting to gRPC.
    """

    def Serve(self, port, pem_dir):
        """
        Sets up the gRPC Server with insecure connection on port
        :param port: port to listen on.
        :param pem_dir: Directory including certificates
        :return: None
        """
        # Create gRPC server
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        SSE.add_ConnectorServicer_to_server(self, server)

        if pem_dir:
            # Secure connection
            with open(os.path.join(pem_dir, 'sse_server_key.pem'), 'rb') as f:
                private_key = f.read()
            with open(os.path.join(pem_dir, 'sse_server_cert.pem'), 'rb') as f:
                cert_chain = f.read()
            with open(os.path.join(pem_dir, 'root_cert.pem'), 'rb') as f:
                root_cert = f.read()
            credentials = grpc.ssl_server_credentials([(private_key, cert_chain)], root_cert, True)
            server.add_secure_port('[::]:{}'.format(port), credentials)
            logging.info('*** Running server in secure mode on port: {} ***'.format(port))
        else:
            # Insecure connection
            server.add_insecure_port('[::]:{}'.format(port))
            logging.info('*** Running server in insecure mode on port: {} ***'.format(port))

        # Start gRPC server
        server.start()
        try:
            while True:
                time.sleep(_ONE_DAY_IN_SECONDS)
        except KeyboardInterrupt:
            server.stop(0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', nargs='?', default='50055')
    parser.add_argument('--pem_dir', nargs='?')
    parser.add_argument('--definition_file', nargs='?', default='functions.json')
    args = parser.parse_args()

    # need to locate the file when script is called from outside it's location dir.
    def_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), args.definition_file)

    calc = ExtensionService(def_file)
    calc.Serve(args.port, args.pem_dir)
