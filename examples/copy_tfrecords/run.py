#Check Python Version
import shutil
import sys
sys.version


#Check TF & TFX Versioning
import tensorflow as tf
print(tf.__version__)
from tfx import v1 as tfx
print(tfx.__version__)






#Setup Variables as examplegen_playground
import os

# Pipeline name
PIPELINE_NAME = "copy_tfrecords"

# Output directory to store artifacts generated from the pipeline.
PIPELINE_ROOT = './artifacts'
# Path to a SQLite DB file to use as an MLMD storage.
METADATA_PATH = os.path.join('metadata', PIPELINE_NAME, 'metadata.db')
# Output directory where created models from the pipeline will be exported.
SERVING_MODEL_DIR = os.path.join('serving_model', PIPELINE_NAME)

# Folder path to data
DATA_ROOT = './data/'

from absl import logging
logging.set_verbosity(logging.INFO)  # Set default logging level.






import json
from typing import Any, Dict, List

from tfx.orchestration.experimental.interactive.interactive_context import InteractiveContext
from tfx.v1.types.standard_artifacts import Examples

from copy_example_gen import component

def _create_pipeline(pipeline_name: str, pipeline_root: str, data_root: str,
                     metadata_path: str) -> tfx.dsl.Pipeline:

  #   example_gen = tfx.components.CsvExampleGen(input_base=data_root)
  tfrecords_dict: Dict[str, str]={
    "train":'gs://vertex-test-bucket-tfx/examples/Split-train/',
    "eval":'gs://vertex-test-bucket-tfx/examples/Split-eval/',
  }

  json_str = json.dumps(tfrecords_dict) 
  print("tfrecords_dict to JSON str: " + json_str)
  print("tfrecords_dict to JSON str: " + str(type(json_str)))



  copy_example=component.CopyExampleGen(input_json_str=json.dumps(tfrecords_dict) )


  # Test downstream component
  statistics_gen = tfx.components.StatisticsGen(
     examples=copy_example.outputs['output_example'])

  # Following three components will be included in the pipeline.
  components = [
    #   example_gen,
      copy_example,
      statistics_gen
  ]

  return tfx.dsl.Pipeline(
      pipeline_name=pipeline_name,
      pipeline_root=pipeline_root,
      metadata_connection_config=tfx.orchestration.metadata
      .sqlite_metadata_connection_config(metadata_path),
      components=components)





tfx.orchestration.LocalDagRunner().run(
  _create_pipeline(
      pipeline_name=PIPELINE_NAME,
      pipeline_root=PIPELINE_ROOT,
      data_root=DATA_ROOT,
      metadata_path=METADATA_PATH)
  )