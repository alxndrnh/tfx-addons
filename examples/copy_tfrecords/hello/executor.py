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

This custom component simply passes examples through. This is meant to serve as
a kind of starting point example for creating custom components.

This component along with other custom component related code will only serve as
an example and will not be supported by TFX team.
"""

import json
import os
import tempfile
import shutil
from typing import Any, Dict, List, Optional, Type, Union


from tfx import types
from tfx.dsl.components.base import base_executor
from tfx.dsl.io import fileio
from tfx.types import artifact_utils
from tfx.utils import io_utils

from ml_metadata import errors
from ml_metadata.proto import metadata_store_pb2

import absl
from tfx import types
from tfx.dsl.components.base import base_driver
from tfx.dsl.components.base import base_node
from tfx.orchestration import data_types
from tfx.orchestration import metadata
from tfx.types import channel_utils
from tfx.utils import doc_controls


from ml_metadata import errors
from ml_metadata.proto import metadata_store_pb2


class Executor(base_executor.BaseExecutor):
  """Executor for HelloComponent."""
  def Do(self,
         input_dict: Dict[str, List[types.Artifact]],
         output_dict: Dict[str, List[types.Artifact]],
         exec_properties: Dict[str, Any]) -> None:


    """Create directory structure of Examples Artifact"""
    split_names=''

    def _parse_tfrecords_dict(tfrecords_dict: dict, split_names: str):
        counter = 1
        tfrecords_dict = exec_properties['tfrecords_dict']
        print(tfrecords_dict)

        destination_examples_artifact_uri = './examples_imported'
        print(destination_examples_artifact_uri)

        if not os.path.exists(destination_examples_artifact_uri):
            os.mkdir(destination_examples_artifact_uri)


        # Create a folder in destination_examples_artifact_uri for every key name
        for key in tfrecords_dict:
              split_name_temp = "Split-"+key
              path = os.path.join(destination_examples_artifact_uri, split_name_temp)
              if not os.path.exists(path):
                  os.mkdir(path)
              shutil.copy(tfrecords_dict[key], path)


        # Create split_names string
        for key in tfrecords_dict:
            if(counter==1):
                split_names+='["'+key+'","'
                counter=counter+1
            elif(counter==2):
                split_names+=key+'"]'
            else:
                split_names+=key+','
                counter=counter+1


    def _set_artifact_properties(artifact: types.Artifact,
                                properties: Optional[Dict[str, Any]],
                                custom_properties: Optional[Dict[str, Any]]):
      """Sets properties and custom_properties to the given artifact."""
      if properties is not None:
        for key, value in properties.items():
          setattr(artifact, key, value)
      if custom_properties is not None:
        for key, value in custom_properties.items():
          if isinstance(value, int):
            artifact.set_int_custom_property(key, value)
          elif isinstance(value, (str, bytes)):
            artifact.set_string_custom_property(key, value)
          else:
            raise NotImplementedError(
                f'Unexpected custom_property value type:{type(value)}')
          


    _parse_tfrecords_dict(exec_properties['tfrecords_dict'], split_names)


    artifact = types.standard_artifacts.Examples
    artifact.is_external = True
    properties={"split_names": '["train","eval"]'}
    custom_properties={}

    _set_artifact_properties(artifact, properties, custom_properties)





    input_artifact = artifact_utils.get_single_instance(
        input_dict['input_data'])
    print("Hello from input_artifact: " + str(input_artifact))
    output_artifact = artifact_utils.get_single_instance(
        output_dict['output_data'])
    print("Hello from output_artifact: " + str(output_artifact))
    output_artifact.split_names = input_artifact.split_names
    print("Hello from output_artifact.split_names: " + str(output_artifact.split_names))

    split_to_instance = {}

    for split in json.loads(input_artifact.split_names):
      uri = artifact_utils.get_split_uri([input_artifact], split)
      split_to_instance[split] = uri

    for split, instance in split_to_instance.items():
      input_dir = instance
      output_dir = artifact_utils.get_split_uri([output_artifact], split)
      for filename in fileio.listdir(input_dir):
        input_uri = os.path.join(input_dir, filename)
        output_uri = os.path.join(output_dir, filename)
        io_utils.copy_file(src=input_uri, dst=output_uri, overwrite=True)


    # absl.logging.info(
    #   'Processing source uri: %s, properties: %s, custom_properties: %s' %
    #   ('./examples/', properties, custom_properties))
    
    # print("From executor.py: " + str(exec_properties))
    # self._log_startup(input_dict, output_dict, exec_properties)

    # metadata_handler= metadata.Metadata
    # mlmd_artifact_type= metadata_store_pb2.ArtifactType
    # print("metadata_handler: " + str(metadata_handler))
    # print("mlmd_artifact_type: " + str(mlmd_artifact_type))



    # mlmd_artifact_type.name = "CopyRecords"
    # mlmd_artifact_type.properties["split"] = metadata_store_pb2.STRING
    # data_type_id = metadata.store.put_artifact_type(mlmd_artifact_type)
    # data_type_id = metadata_handler.store
    # print("data_type_id: " + str(data_type_id))

    # mlmd_artifact_type_id = metadata_handler.publish_artifacts(mlmd_artifact_type)
    # print("data_type_id: " + str(mlmd_artifact_type_id))



        # #   cwd = os.getcwd()
        # #   temp_dir = tempfile.TemporaryDirectory(dir = cwd)

        # #   print(temp_dir.name)

        # # Destination directory for source
        # destination_examples_artifact_uri = './examples_imported'
        # print(destination_examples_artifact_uri)

        # if not os.path.exists(destination_examples_artifact_uri):
        #     os.mkdir(destination_examples_artifact_uri)


        # # Create a folder in destination_examples_artifact_uri for every key name
        # for key in tfrecords_dict:
        #         split_name_temp = "Split-"+key
        #         path = os.path.join(destination_examples_artifact_uri, split_name_temp)
        #         if not os.path.exists(path):
        #             os.mkdir(path)
        #             print(path)
        #         shutil.copy(tfrecords_dict[key], path)
        #         print(path)


        # # Create split_names string
        # for key in tfrecords_dict:
        #     if(counter==1):
        #         split_names+='["'+key+'","'
        #         counter=counter+1
        #     elif(counter==2):
        #         split_names+=key+'"]'
        #     else:
        #         split_names+=key+','
        #         counter=counter+1

        # print(split_names)



import re
'./examples/'
dict_output = {
	"name": "NotFoundError",
	"message": "",
	"stack": "\u001b[0;31m---------------------------------------------------------------------------\u001b[0m\n\u001b[0;31mNotFoundError\u001b[0m                             Traceback (most recent call last)\n\u001b[0;32m~/.pyenv/versions/3.8.2/envs/tfx-3.8.2/lib/python3.8/site-packages/tfx/dsl/io/plugins/tensorflow_gfile.py\u001b[0m in \u001b[0;36mmkdir\u001b[0;34m(path)\u001b[0m\n\u001b[1;32m     75\u001b[0m       \u001b[0;32mtry\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m---> 76\u001b[0;31m         \u001b[0mtf\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mio\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mgfile\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mmkdir\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mpath\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m     77\u001b[0m       \u001b[0;32mexcept\u001b[0m \u001b[0mtf\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0merrors\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mNotFoundError\u001b[0m \u001b[0;32mas\u001b[0m \u001b[0me\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\n\u001b[0;32m~/.pyenv/versions/3.8.2/envs/tfx-3.8.2/lib/python3.8/site-packages/tensorflow/python/lib/io/file_io.py\u001b[0m in \u001b[0;36mcreate_dir_v2\u001b[0;34m(path)\u001b[0m\n\u001b[1;32m    482\u001b[0m   \"\"\"\n\u001b[0;32m--> 483\u001b[0;31m   \u001b[0m_pywrap_file_io\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mCreateDir\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mcompat\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mpath_to_bytes\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mpath\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m    484\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\n\u001b[0;31mNotFoundError\u001b[0m: /tmp/tfx-interactive-2023-03-13T18_58_59.825612-ot2c9dut/CopyExampleGen/output_example/1/Split-eval/); No such file or directory\n\nThe above exception was the direct cause of the following exception:\n\n\u001b[0;31mNotFoundError\u001b[0m                             Traceback (most recent call last)\n\u001b[0;32m/tmp/ipykernel_2140447/2256786037.py\u001b[0m in \u001b[0;36m<cell line: 15>\u001b[0;34m()\u001b[0m\n\u001b[1;32m     13\u001b[0m \u001b[0mcopy_examples\u001b[0m\u001b[0;34m=\u001b[0m\u001b[0mCopyExampleGen\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0minput_dict\u001b[0m\u001b[0;34m=\u001b[0m\u001b[0mtfrecords_dict\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m     14\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m---> 15\u001b[0;31m \u001b[0mcontext\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mrun\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mcopy_examples\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m     16\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m     17\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\n\u001b[0;32m~/.pyenv/versions/3.8.2/envs/tfx-3.8.2/lib/python3.8/site-packages/tfx/orchestration/experimental/interactive/notebook_utils.py\u001b[0m in \u001b[0;36mrun_if_ipython\u001b[0;34m(*args, **kwargs)\u001b[0m\n\u001b[1;32m     29\u001b[0m       \u001b[0;31m# __IPYTHON__ variable is set by IPython, see\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m     30\u001b[0m       \u001b[0;31m# https://ipython.org/ipython-doc/rel-0.10.2/html/interactive/reference.html#embedding-ipython.\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m---> 31\u001b[0;31m       \u001b[0;32mreturn\u001b[0m \u001b[0mfn\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;34m*\u001b[0m\u001b[0margs\u001b[0m\u001b[0;34m,\u001b[0m \u001b[0;34m**\u001b[0m\u001b[0mkwargs\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m     32\u001b[0m     \u001b[0;32melse\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m     33\u001b[0m       logging.warning(\n\n\u001b[0;32m~/.pyenv/versions/3.8.2/envs/tfx-3.8.2/lib/python3.8/site-packages/tfx/orchestration/experimental/interactive/interactive_context.py\u001b[0m in \u001b[0;36mrun\u001b[0;34m(self, component, enable_cache, beam_pipeline_args)\u001b[0m\n\u001b[1;32m    162\u001b[0m         \u001b[0mtelemetry_utils\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mLABEL_TFX_RUNNER\u001b[0m\u001b[0;34m:\u001b[0m \u001b[0mrunner_label\u001b[0m\u001b[0;34m,\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    163\u001b[0m     }):\n\u001b[0;32m--> 164\u001b[0;31m       \u001b[0mexecution_id\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mlauncher\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mlaunch\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mexecution_id\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m    165\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    166\u001b[0m     return execution_result.ExecutionResult(\n\n\u001b[0;32m~/.pyenv/versions/3.8.2/envs/tfx-3.8.2/lib/python3.8/site-packages/tfx/orchestration/launcher/base_component_launcher.py\u001b[0m in \u001b[0;36mlaunch\u001b[0;34m(self)\u001b[0m\n\u001b[1;32m    204\u001b[0m       \u001b[0;31m# be immutable in this context.\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    205\u001b[0m       \u001b[0;31m# output_dict can still be changed, specifically properties.\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m--> 206\u001b[0;31m       self._run_executor(execution_decision.execution_id,\n\u001b[0m\u001b[1;32m    207\u001b[0m                          \u001b[0mcopy\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mdeepcopy\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mexecution_decision\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0minput_dict\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m,\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    208\u001b[0m                          \u001b[0mexecution_decision\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0moutput_dict\u001b[0m\u001b[0;34m,\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\n\u001b[0;32m~/.pyenv/versions/3.8.2/envs/tfx-3.8.2/lib/python3.8/site-packages/tfx/orchestration/launcher/in_process_component_launcher.py\u001b[0m in \u001b[0;36m_run_executor\u001b[0;34m(self, execution_id, input_dict, output_dict, exec_properties)\u001b[0m\n\u001b[1;32m     71\u001b[0m     \u001b[0;31m# be immutable in this context.\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m     72\u001b[0m     \u001b[0;31m# output_dict can still be changed, specifically properties.\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m---> 73\u001b[0;31m     executor.Do(\n\u001b[0m\u001b[1;32m     74\u001b[0m         copy.deepcopy(input_dict), output_dict, copy.deepcopy(exec_properties))\n\n\u001b[0;32m~/.pyenv/versions/3.8.2/envs/tfx-3.8.2/lib/python3.8/site-packages/tfx/dsl/component/experimental/decorators.py\u001b[0m in \u001b[0;36mDo\u001b[0;34m(self, input_dict, output_dict, exec_properties)\u001b[0m\n\u001b[1;32m    229\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    230\u001b[0m     \u001b[0;31m# Call function and check returned values.\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m--> 231\u001b[0;31m     \u001b[0moutputs\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_FUNCTION\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;34m**\u001b[0m\u001b[0mfunction_args\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m    232\u001b[0m     \u001b[0moutputs\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0moutputs\u001b[0m \u001b[0;32mor\u001b[0m \u001b[0;34m{\u001b[0m\u001b[0;34m}\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    233\u001b[0m     output_dict.update(\n\n\u001b[0;32m/tmp/ipykernel_2140447/1283998052.py\u001b[0m in \u001b[0;36mCopyExampleGen\u001b[0;34m(input_dict, output_example)\u001b[0m\n\u001b[1;32m     25\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m     26\u001b[0m         \u001b[0mfileio\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mmkdir\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0moutput_example\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0muri\u001b[0m\u001b[0;34m+\u001b[0m\u001b[0;34m'/Split-train/'\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m---> 27\u001b[0;31m         \u001b[0mfileio\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mmkdir\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0moutput_example\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0muri\u001b[0m\u001b[0;34m+\u001b[0m\u001b[0;34m'/Split-eval/)'\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m     28\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m     29\u001b[0m         \u001b[0mfileio\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mcopy\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0minput_train_dir\u001b[0m\u001b[0;34m,\u001b[0m \u001b[0moutput_example\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0muri\u001b[0m\u001b[0;34m+\u001b[0m\u001b[0;34m'/Split-train/data_tfrecord-00000-of-00001.gz)'\u001b[0m\u001b[0;34m,\u001b[0m \u001b[0;32mTrue\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\n\u001b[0;32m~/.pyenv/versions/3.8.2/envs/tfx-3.8.2/lib/python3.8/site-packages/tfx/dsl/io/fileio.py\u001b[0m in \u001b[0;36mmkdir\u001b[0;34m(path)\u001b[0m\n\u001b[1;32m     83\u001b[0m \u001b[0;32mdef\u001b[0m \u001b[0mmkdir\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mpath\u001b[0m\u001b[0;34m:\u001b[0m \u001b[0mPathType\u001b[0m\u001b[0;34m)\u001b[0m \u001b[0;34m->\u001b[0m \u001b[0;32mNone\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m     84\u001b[0m   \u001b[0;34m\"\"\"Make a directory at the given path; parent directory must exist.\"\"\"\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m---> 85\u001b[0;31m   \u001b[0m_get_filesystem\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mpath\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mmkdir\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mpath\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m     86\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m     87\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\n\u001b[0;32m~/.pyenv/versions/3.8.2/envs/tfx-3.8.2/lib/python3.8/site-packages/tfx/dsl/io/plugins/tensorflow_gfile.py\u001b[0m in \u001b[0;36mmkdir\u001b[0;34m(path)\u001b[0m\n\u001b[1;32m     76\u001b[0m         \u001b[0mtf\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mio\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mgfile\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mmkdir\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mpath\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m     77\u001b[0m       \u001b[0;32mexcept\u001b[0m \u001b[0mtf\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0merrors\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mNotFoundError\u001b[0m \u001b[0;32mas\u001b[0m \u001b[0me\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m---> 78\u001b[0;31m         \u001b[0;32mraise\u001b[0m \u001b[0mfilesystem\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mNotFoundError\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;34m)\u001b[0m \u001b[0;32mfrom\u001b[0m \u001b[0me\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m     79\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m     80\u001b[0m     \u001b[0;34m@\u001b[0m\u001b[0mstaticmethod\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\n\u001b[0;31mNotFoundError\u001b[0m: "
}
ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
result = ansi_escape.sub('', dict_output["stack"])

print(result)