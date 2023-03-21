# Copyright 2019 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Example of a Hello World TFX custom component.

This custom component simply reads tf.Examples from input and passes through as
output.  This is meant to serve as a kind of starting point example for creating
custom components.

This component along with other custom component related code will only serve as
an example and will not be supported by TFX team.
"""
import os
import tensorflow as tf
print('TensorFlow version: {}'.format(tf.__version__))
from tfx import v1 as tfx
print('TFX version: {}'.format(tfx.__version__))
from typing import List
from tfx.v1.types.standard_artifacts import Examples
from tfx.dsl.component.experimental.decorators import component
from tfx.dsl.io import fileio
import logging
import json

def _split_names_string_builder(split_names_list: List):
    str1 = "["
    urlist_len = len(split_names_list)-1
    index = 0

    for ele in split_names_list:
        if(index==urlist_len):
            str1 += "\""+ele+"\""+"]"
            break
        str1 += "\""+ele+"\""+","
        index+=1
    return str1

@component
def CopyExampleGen(
        input_json_str: tfx.dsl.components.Parameter[str],
        output_example: tfx.dsl.components.OutputArtifact[Examples]
      ) -> tfx.dsl.components.OutputDict():
        
        input_dict = json.loads(input_json_str)
        print("JSON1 DATA: " + str(input_dict))
        print("JSON1 DATA TYPE: " + str(type(input_dict)))

        
        logging.warning(f"INPUT_DICT: {type(input_dict)}")
        logging.warning(f"INPUT_DICT: {input_dict}")

        """Parse input_dict: creates a directory from the split-names and tfrecord uris provided"""
        split_names=[]
        for key, value in input_dict.items():
          split_names.append(key)
          logging.warning(f"SPLIT_NAME: {split_names}")
          logging.warning(f"KEY: {key}")
          logging.warning(f"VALUE: {value}")
        
        logging.warning(f"SPLIT_NAMES: {type(split_names)}")
        logging.warning(f"SPLIT_NAMES: {split_names}")

        split_names_string=_split_names_string_builder(split_names)
        output_example.split_names=str(split_names_string)
        
        """Make directories"""
        tfrecords_list=[]
        output_example_uri=output_example.uri
        
        logging.warning(f"INPUT_DICT: {type(input_dict)}")
        logging.warning(f"OUTPUT_EXAMPLE_URI_TYPE: {type(output_example_uri)}")
        logging.warning(f"OUTPUT_EXAMPLE_URI: {output_example_uri}")

        for key, value in input_dict.items():
          logging.warning(f"Key: {key}")
          split_value=(f"/Split-{key}/")
          logging.warning(output_example_uri)
          logging.warning(f"{output_example_uri}{split_value}")
          fileio.mkdir(f"{output_example_uri}{split_value}")
          logging.warning(f"input_dict: {str(input_dict)}")
          # tfrecords_list=fileio.glob(input_dict[key]+'*.gz')
          tfrecords_list=fileio.glob(f"{input_dict[key]}*.gz")
          logging.warning(f"tfrecords_list: {str(tfrecords_list)}")

          """"Copy files into directories"""
          for tfrecord in tfrecords_list:
                """TODO: Find a better way to extra file name"""
                file_name=os.path.basename(os.path.normpath(tfrecord))
                fileio.copy(tfrecord, output_example.uri+split_value+file_name, True)
                # print("file_name: "+file_name)