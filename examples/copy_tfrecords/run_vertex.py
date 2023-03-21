import os
from tfx import v1 as tfx
from typing import Dict
from absl import logging
from tfx import v1 as tfx
from google.cloud import aiplatform
from google.cloud.aiplatform import pipeline_jobs
import logging
from copy_example_gen import component


# cwd = os.getcwd()
# print(cwd)
""" """
GOOGLE_CLOUD_PROJECT = 'alex-tfx-dev'
GOOGLE_CLOUD_REGION = 'us-west1'
GCS_BUCKET_NAME = 'vertex-test-bucket-tfx'

if not (GOOGLE_CLOUD_PROJECT and GOOGLE_CLOUD_REGION and GCS_BUCKET_NAME):
    from absl import logging
    logging.error('Please set all required parameters.')

PIPELINE_NAME = 'vertex-test'

# Path to various pipeline artifact.
PIPELINE_ROOT = 'gs://{}/pipeline_root/{}'.format(GCS_BUCKET_NAME, PIPELINE_NAME)

# Paths for users' Python module.
MODULE_ROOT = 'gs://{}/pipeline_module/{}'.format(GCS_BUCKET_NAME, PIPELINE_NAME)

# Paths for users' data.
DATA_ROOT = 'gs://{}/data/{}'.format(GCS_BUCKET_NAME, PIPELINE_NAME)

# This is the path where your model will be pushed for serving.
SERVING_MODEL_DIR = 'gs://{}/serving_model/{}'.format(
    GCS_BUCKET_NAME, PIPELINE_NAME)

_trainer_module_file = 'penguin_trainer.py'

# Copied from https://www.tensorflow.org/tfx/tutorials/tfx/penguin_simple and
# slightly modified because we don't need `metadata_path` argument.

def _create_pipeline(pipeline_name: str, pipeline_root: str, data_root: str,
                     module_file: str, serving_model_dir: str,
                     ) -> tfx.dsl.Pipeline:
  # Brings data into the pipeline.
  # User input dictionary with split-names and their resepctive uri to tfrecords
  tfrecords_dict: Dict[str, str]={
    "train":'gs://vertex-test-bucket-tfx/examples/Split-train/',
    "eval":'gs://vertex-test-bucket-tfx/examples/Split-eval/',
  }

  # copy_example = component.CopyExampleGen(
  #     input_dict=tfrecords_dict
  # )
  example_gen = tfx.components.CsvExampleGen(input_base=data_root)

  # Uses user-provided Python function that trains a model.
  trainer = tfx.components.Trainer(
      module_file=module_file,
      # examples=copy_example.outputs['output_example'],
      examples=example_gen.outputs['examples'],
      train_args=tfx.proto.TrainArgs(num_steps=100),
      eval_args=tfx.proto.EvalArgs(num_steps=5))

  # Pushes the model to a filesystem destination.
  pusher = tfx.components.Pusher(
      model=trainer.outputs['model'],
      push_destination=tfx.proto.PushDestination(
          filesystem=tfx.proto.PushDestination.Filesystem(
              base_directory=serving_model_dir)))

  # Following three components will be included in the pipeline.
  components = [
      # copy_example,
      example_gen,
      trainer,
      pusher,
  ]

  return tfx.dsl.Pipeline(
      pipeline_name=pipeline_name,
      pipeline_root=pipeline_root,
      components=components)
# docs_infra: no_execute

PIPELINE_DEFINITION_FILE = PIPELINE_NAME + '_pipeline.json'

runner = tfx.orchestration.experimental.KubeflowV2DagRunner(
    config=tfx.orchestration.experimental.KubeflowV2DagRunnerConfig(default_image='us-west1-docker.pkg.dev/alex-tfx-dev/docker/custom-component-image'),
    output_filename=PIPELINE_DEFINITION_FILE)
# Following function will write the pipeline definition to PIPELINE_DEFINITION_FILE.
_ = runner.run(
    _create_pipeline(
        pipeline_name=PIPELINE_NAME,
        pipeline_root=PIPELINE_ROOT,
        data_root=DATA_ROOT,
        module_file=os.path.join(MODULE_ROOT, _trainer_module_file),
        serving_model_dir=SERVING_MODEL_DIR))

logging.getLogger().setLevel(logging.INFO)

aiplatform.init(project=GOOGLE_CLOUD_PROJECT, location=GOOGLE_CLOUD_REGION)

job = pipeline_jobs.PipelineJob(template_path=PIPELINE_DEFINITION_FILE,
                                display_name=PIPELINE_NAME)
job.submit()
