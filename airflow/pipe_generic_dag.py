from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow_ext.gfw import config as config_tools
from airflow_ext.gfw.models import DagFactory

from datetime import datetime

from jsonschema import validate

import imp
import json
import logging
import os
import posixpath as pp


PIPELINE='pipe_vms_generic'

def get_dag_path(pipeline, module=None):
    """
    Gets the DAG path.

    :@param pipeline: The Airflow Variable key that has the config.
    :@type pipeline: str.
    :@param module: The module that belongs to the pipeline.
    :@type module: str.
    :@return: The DAG path of the pipeline.
    """
    if module is None:
        module = pipeline
    config = Variable.get(pipeline, deserialize_json=True)
    return pp.join(config['dag_install_path'], '{}_dag.py'.format(module))

pipe_segment = imp.load_source('pipe_segment', get_dag_path('pipe_segment'))
pipe_measures = imp.load_source('pipe_measures', get_dag_path('pipe_measures'))
pipe_anchorages = imp.load_source('pipe_anchorages', get_dag_path('pipe_anchorages'))
pipe_encounters = imp.load_source('pipe_encounters', get_dag_path('pipe_encounters'))
pipe_features = imp.load_source('pipe_features', get_dag_path('pipe_features'))
pipe_events_anchorages = imp.load_source('pipe_events_anchorages', get_dag_path('pipe_events.anchorages','pipe_events_anchorages'))
pipe_events_encounters = imp.load_source('pipe_events_encounters', get_dag_path('pipe_events.encounters','pipe_events_encounters'))
pipe_events_fishing = imp.load_source('pipe_events_fishing', get_dag_path('pipe_events.fishing','pipe_events_fishing'))


#
# PIPE_VMS_GENERIC
#
class VMSGenericDagFactory(DagFactory):
    """Concrete class to handle the DAG for pipe_vms_generic."""

    def __init__(self, name, **kwargs):
        """
        Constructs the DAG.

        :@param pipeline: The pipeline name. Default value the PIPELINE.
        :@type pipeline: str.
        :@param kwargs: A dict of optional parameters.
        :@param kwargs: dict.
        """
        super(VMSGenericDagFactory, self).__init__(pipeline=PIPELINE, **kwargs)
        self.pipeline = '{}_{}'.format(PIPELINE, name)


    def build(self, dag_id):
        """
        Override of build method.

        :@param dag_id: The id of the DAG.
        :@type dag_id: str.
        """

        config = self.config
        config['source_dataset'] = config['pipeline_dataset']
        config['source_tables'] = config['normalized_tables']

        default_args = self.default_args

        subdag_default_args = dict(
            start_date=default_args['start_date'],
            end_date=default_args['end_date']
        )
        subdag_config = dict(
            pipeline_dataset=config['pipeline_dataset'],
            source_dataset=config['pipeline_dataset'],
            events_dataset=config['events_dataset'],
            dataflow_runner='{dataflow_runner}'.format(**config),
            temp_shards_per_day="3",
        )
        config['source_paths'] = ','.join(self.source_table_paths())
        config['source_dates'] = ','.join(self.source_date_range())

        with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:

            source_sensors = self.source_table_sensors(dag)

            segment = SubDagOperator(
                subdag=pipe_segment.PipeSegmentDagFactory(
                    schedule_interval=dag.schedule_interval,
                    extra_default_args=subdag_default_args,
                    extra_config=dict(
                        pipeline_dataset=config['pipeline_dataset'],
                        source_dataset=config['pipeline_dataset'],
                        source_tables='{normalized_tables}'.format(**config),
                        dataflow_runner='{dataflow_runner}'.format(**config),
                        temp_shards_per_day="3",
                    )
                ).build(dag_id=dag_id+'.segment'),
                trigger_rule=TriggerRule.ONE_SUCCESS,
                depends_on_past=True,
                task_id='segment'
            )

            measures = SubDagOperator(
                subdag=pipe_measures.PipeMeasuresDagFactory(
                    schedule_interval=dag.schedule_interval,
                    extra_default_args=subdag_default_args,
                    extra_config=subdag_config
                ).build(dag_id=dag_id+'.measures'),
                task_id='measures'
            )

            port_events = SubDagOperator(
                subdag=pipe_anchorages.PipeAnchoragesPortEventsDagFactory(
                    schedule_interval=dag.schedule_interval,
                    extra_default_args=subdag_default_args,
                    extra_config=subdag_config
                ).build(dag_id=dag_id+'.port_events'),
                task_id='port_events'
            )

            port_visits = SubDagOperator(
                subdag=pipe_anchorages.PipeAnchoragesPortVisitsDagFactory(
                    schedule_interval=dag.schedule_interval,
                    extra_default_args=subdag_default_args,
                    extra_config=subdag_config
                ).build(dag_id=dag_id+'.port_visits'),
                task_id='port_visits'
            )

            encounters = SubDagOperator(
                subdag=pipe_encounters.PipeEncountersDagFactory(
                    schedule_interval=dag.schedule_interval,
                    extra_default_args=subdag_default_args,
                    extra_config=subdag_config
                ).build(dag_id=dag_id+'.encounters'),
                task_id='encounters'
            )


            for sensor in source_sensors:
                dag >> sensor >> segment >> measures

            measures >> port_events >> port_visits
            measures >> encounters

            if config.get('enable_features', False):

                features = SubDagOperator(
                    subdag=pipe_features.PipeFeaturesDagFactory(
                        schedule_interval=dag.schedule_interval,
                        extra_default_args=subdag_default_args,
                        extra_config=subdag_config
                    ).build(dag_id=dag_id+'.features'),
                    depends_on_past=True,
                    task_id='features'
                )

                port_visits >> features
                encounters >> features

                if config.get('enable_events', False):
                    events_anchorages = SubDagOperator(
                        subdag = pipe_events_anchorages.PipelineDagFactory(
                            config_tools.load_config('pipe_events.anchorages'),
                            schedule_interval=dag.schedule_interval,
                            extra_default_args=subdag_default_args,
                            extra_config=subdag_config
                        ).build(dag_id=dag_id+'.pipe_events_anchorages'),
                        depends_on_past=True,
                        task_id='pipe_events_anchorages'
                    )

                    events_encounters = SubDagOperator(
                        subdag = pipe_events_encounters.PipelineDagFactory(
                            config_tools.load_config('pipe_events.encounters'),
                            schedule_interval=dag.schedule_interval,
                            extra_default_args=subdag_default_args,
                            extra_config=subdag_config
                        ).build(dag_id=dag_id+'.pipe_events_encounters'),
                        depends_on_past=True,
                        task_id='pipe_events_encounters'
                    )

                    events_fishing = SubDagOperator(
                        subdag = pipe_events_fishing.PipelineDagFactory(
                            config_tools.load_config('pipe_events.fishing'),
                            schedule_interval=dag.schedule_interval,
                            extra_default_args=subdag_default_args,
                            extra_config=subdag_config
                        ).build(dag_id=dag_id+'.pipe_events_fishing'),
                        depends_on_past=True,
                        task_id='pipe_events_fishing'
                    )

                    # Points to each independent event
                    features >> events_anchorages
                    features >> events_encounters
                    features >> events_fishing

        return dag

def validateJson(data):
    """
    Validates the configuration with a JSON schema.

    :@param data: The data to be validated.
    :@type data: dict.
    :raise: Error in case the dict don't match the schema.
    """
    folder=os.path.abspath(os.path.dirname(__file__))
    with open('{}/{}'.format(folder,"schemas/vms_list_schema.json")) as vms_schema:
        validate(instance=data, schema=json.loads(vms_schema.read()))

variables = config_tools.load_config(PIPELINE)
validateJson(variables)
for vms in variables['vms_list']:
    for mode in ['daily','monthly', 'yearly']:
        print('>>>>>> VMS: {}'.format(vms))
        pipeline_start_date = datetime.strptime(vms['start_date'].strip(), "%Y-%m-%d")
        dag_id = '{}_{}_{}'.format(PIPELINE, vms['name'], mode)
        globals()[dag_id] = VMSGenericDagFactory(vms['name'], schedule_interval='@{}'.format(mode), extra_default_args={'start_date':pipeline_start_date}, extra_config=vms).build(dag_id)
