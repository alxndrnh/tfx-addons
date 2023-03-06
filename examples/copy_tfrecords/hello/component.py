from typing import Optional

from tfx import types
from tfx.dsl.components.base import base_component
from tfx.dsl.components.base import executor_spec
from hello import executor
from tfx.types import channel_utils
from tfx.types import standard_artifacts
from tfx.types.component_spec import ChannelParameter
from tfx.types.component_spec import ExecutionParameter


class HelloComponentSpec(types.ComponentSpec):
  PARAMETERS = {
      # These are parameters that will be passed in the call to
      # create an instance of this component.
      'tfrecords_dict': ExecutionParameter(type=dict)
  }
  INPUTS = {
      # This will be a dictionary with input artifacts, including URIs
      'input_data': ChannelParameter(type=standard_artifacts.Examples),
  }
  OUTPUTS = {
      # This will be a dictionary which this component will populate
      'output_data': ChannelParameter(type=standard_artifacts.Examples),
  }
  

class HelloComponent(base_component.BaseComponent):
  SPEC_CLASS = HelloComponentSpec
  EXECUTOR_SPEC = executor_spec.ExecutorClassSpec(executor.Executor)

  def __init__(self,
               input_data: types.Channel = None,
               output_data: types.Channel = None,
               tfrecords_dict: dict = None):

    if not output_data:
      output_data = channel_utils.as_channel([standard_artifacts.Examples()])

    spec = HelloComponentSpec(input_data=input_data,
                              output_data=output_data,
                              tfrecords_dict=tfrecords_dict)
    super().__init__(spec=spec)