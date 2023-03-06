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
    
    print("From executor.py: " + str(exec_properties))
    self._log_startup(input_dict, output_dict, exec_properties)

    metadata_handler= metadata.Metadata
    mlmd_artifact_type= metadata_store_pb2.ArtifactType
    print("metadata_handler: " + str(metadata_handler))
    print("mlmd_artifact_type: " + str(mlmd_artifact_type))

    # mlmd_artifact_type.name = "CopyRecords"
    # mlmd_artifact_type.properties["split"] = metadata_store_pb2.STRING
    # data_type_id = metadata.store.put_artifact_type(mlmd_artifact_type)
    data_type_id = metadata_handler.store
    print("data_type_id: " + str(data_type_id))

    # mlmd_artifact_type_id = metadata_handler.publish_artifacts(mlmd_artifact_type)
    # print("data_type_id: " + str(mlmd_artifact_type_id))










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