import tfx
import os

from typing import Dict, List
from tfx.v1.types.standard_artifacts import Examples
from tfx.types import artifact_utils
from typing import Dict

from tfx.dsl.component.experimental.decorators import component

from tfx.dsl.io import fileio

def _string_builder(split_names_list: List):
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
        input_dict: tfx.dsl.components.Parameter[Dict[str, str]],
        output_example: tfx.dsl.components.OutputArtifact[Examples]
      ) -> tfx.dsl.components.OutputDict():
    
        """Parse input_dict: creates a directory from the split-names and tfrecord uris provided"""
        split_names=[]
        for key in input_dict:
          split_names.append(key)
          
        split_names_string=_string_builder(split_names)
        output_example.split_names=str(split_names_string)
        
        """Make directories"""
        tfrecords_list=[]
        for key in input_dict:
          split_value="/Split-"+key+"/"
          fileio.mkdir(output_example.uri+split_value)
          tfrecords_list=fileio.glob(input_dict[key]+'*.gz')
          # print("tfrecords_list: " + str(tfrecords_list))

          """"Copy files into directories"""
          for tfrecord in tfrecords_list:
                """TODO: Find a better way to extra file name"""
                file_name=os.path.basename(os.path.normpath(tfrecord))
                fileio.copy(tfrecord, output_example.uri+split_value+file_name, True)
                # print("file_name: "+file_name)