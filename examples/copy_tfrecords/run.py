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

# Folder path to tfrecords
TFRECORDS_TRAIN_DATA_PATH = '../../../../Documents/tfrecord1_split_train/data_tfrecord-00000-of-00001.gz'
TFRECORDS_EVAL_DATA_PATH = '../../../../Documents/tfrecord2_split_eval/data_tfrecord-00000-of-00001.gz'

from absl import logging
logging.set_verbosity(logging.INFO)  # Set default logging level.






import json
from typing import Any, Dict, List

from tfx.orchestration.experimental.interactive.interactive_context import InteractiveContext
from tfx.v1.types.standard_artifacts import Examples

from hello import component

def _create_pipeline(pipeline_name: str, pipeline_root: str, data_root: str,
                     metadata_path: str) -> tfx.dsl.Pipeline:
  # User input dictionary with split-names and their resepctive uri to tfrecords
  tfrecords_dict={
    "train":TFRECORDS_TRAIN_DATA_PATH,
    "eval":TFRECORDS_EVAL_DATA_PATH
  }
  split_names=''
  counter=1

  # Destination directory for source
  destination_examples_artifact_uri = './examples/'

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

  print(split_names)
  
  # CopyExampleGen execution process to replace Importer Node functionality
  examples_importer = tfx.dsl.Importer(
        source_uri=destination_examples_artifact_uri,
        artifact_type=tfx.types.standard_artifacts.Examples,
        # properties={"split_names": '["train","eva"]'}
        properties={"split_names": split_names}
        ).with_id('examples_importer')


  hello_component = component.HelloComponent(
      name='hi',
      input_data=examples_importer.outputs['result'],
      splits=tfrecords_dict
    #   input_data=tfrecords_dict
  )

  # Test downstream component
  statistics_gen = tfx.components.StatisticsGen(
     examples=hello_component.outputs['output_data'])

  # Following three components will be included in the pipeline.
  components = [
      # example_gen,
      examples_importer,
      hello_component,
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