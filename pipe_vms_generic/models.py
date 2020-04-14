from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow_ext.gfw import config as config_tools
from airflow_ext.gfw.models import DagFactory

import imp
import posixpath as pp


def get_dag_path(pipeline, module=None):
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
pipe_events_gaps = imp.load_source('pipe_events_gaps', get_dag_path('pipe_events.gaps','pipe_events_gaps'))


#
# PIPE_VMS_GENERIC
#
class VMSGenericDagFactory(DagFactory):
    def __init__(self, pipeline, **kwargs):
        super(VMSGenericDagFactory, self).__init__(pipeline=pipeline, **kwargs)

    def source_validate(self, dag):
        """
        Validates the necessary sources to proceed with the VMS pipeline

        :param dag: The DAG which will be associated with the validation.
        :type dag: DAG from Airflow.
        """
        raise NotImplementedError

    def fetch_normalize(self, dag):
        """
        Fetches the data and normalizes using airflow operators.

        :param dag: The DAG which will be associated with the validation.
        :type dag: DAG from Airflow.
        """
        raise NotImplementedError

    def pre_processing(self, dag):
        """
        Stablishes the chain dependency between the source validators and the fetch_normalization

        :param dag: The DAG which will be associated with the validation.
        :type dag: DAG from Airflow.
        """
        fetch_normalization = fetch_normalize(dag)

        for sensor in self.source_validate(dag):
            dag >> sensor >> fetch_normalization

        return fetch_normalization


    def build(self, mode):
        dag_id = '{}_{}'.format(self.pipeline, mode)

        config = self.config
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

            segment = SubDagOperator(
                subdag=pipe_segment.PipeSegmentDagFactory(
                    schedule_interval=dag.schedule_interval,
                    extra_default_args=subdag_default_args,
                    extra_config=dict(
                        pipeline_dataset=config['pipeline_dataset'],
                        source_dataset=config['pipeline_dataset'],
                        normalized_tables='{normalized}'.format(**config),
                        dataflow_runner='{dataflow_runner}'.format(**config),
                        temp_shards_per_day="3",
                    )
                ).build(dag_id='{}.segment'.format(dag_id)),
                trigger_rule=TriggerRule.ONE_SUCCESS,
                depends_on_past=True,
                task_id='segment'
            )

            measures = SubDagOperator(
                subdag=pipe_measures.PipeMeasuresDagFactory(
                    schedule_interval=dag.schedule_interval,
                    extra_default_args=subdag_default_args,
                    extra_config=subdag_config
                ).build(dag_id='{}.measures'.format(dag_id)),
                task_id='measures'
            )

            port_events = SubDagOperator(
                subdag=pipe_anchorages.PipeAnchoragesPortEventsDagFactory(
                    schedule_interval=dag.schedule_interval,
                    extra_default_args=subdag_default_args,
                    extra_config=subdag_config
                ).build(dag_id='{}.port_events'.format(dag_id)),
                task_id='port_events'
            )

            port_visits = SubDagOperator(
                subdag=pipe_anchorages.PipeAnchoragesPortVisitsDagFactory(
                    schedule_interval=dag.schedule_interval,
                    extra_default_args=subdag_default_args,
                    extra_config=subdag_config
                ).build(dag_id='{}.port_visits'.format(dag_id)),
                task_id='port_visits'
            )

            encounters = SubDagOperator(
                subdag=pipe_encounters.PipeEncountersDagFactory(
                    schedule_interval=dag.schedule_interval,
                    extra_default_args=subdag_default_args,
                    extra_config=subdag_config
                ).build(dag_id='{}.encounters'.format(dag_id)),
                task_id='encounters'
            )


            self.pre_processing(dag) >> segment >> measures

            measures >> port_events >> port_visits
            measures >> encounters

            if config.get('enable_features_events', False):

                features = SubDagOperator(
                    subdag=pipe_features.PipeFeaturesDagFactory(
                        schedule_interval=dag.schedule_interval,
                        extra_default_args=subdag_default_args,
                        extra_config=subdag_config
                    ).build(dag_id='{}.features'.format(dag_id)),
                    depends_on_past=True,
                    task_id='features'
                )

                events_anchorages = SubDagOperator(
                    subdag = pipe_events_anchorages.PipelineDagFactory(
                        config_tools.load_config('pipe_events.anchorages'),
                        schedule_interval=dag.schedule_interval,
                        extra_default_args=subdag_default_args,
                        extra_config=subdag_config
                    ).build(dag_id='{}.pipe_events_anchorages'.format(dag_id)),
                    depends_on_past=True,
                    task_id='pipe_events_anchorages'
                )

                events_encounters = SubDagOperator(
                    subdag = pipe_events_encounters.PipelineDagFactory(
                        config_tools.load_config('pipe_events.encounters'),
                        schedule_interval=dag.schedule_interval,
                        extra_default_args=subdag_default_args,
                        extra_config=subdag_config
                    ).build(dag_id='{}.pipe_events_encounters'.format(dag_id)),
                    depends_on_past=True,
                    task_id='pipe_events_encounters'
                )

                events_fishing = SubDagOperator(
                    subdag = pipe_events_fishing.PipelineDagFactory(
                        config_tools.load_config('pipe_events.fishing'),
                        schedule_interval=dag.schedule_interval,
                        extra_default_args=subdag_default_args,
                        extra_config=subdag_config
                    ).build(dag_id='{}.pipe_events_fishing'.format(dag_id)),
                    depends_on_past=True,
                    task_id='pipe_events_fishing'
                )

                events_gaps = SubDagOperator(
                    subdag = pipe_events_gaps.PipelineDagFactory(
                        config_tools.load_config('pipe_events.gaps'),
                        schedule_interval=dag.schedule_interval,
                        extra_default_args=subdag_default_args,
                        extra_config=subdag_config
                    ).build(dag_id='{}.pipe_events_gaps'.format(dag_id)),
                    depends_on_past=True,
                    task_id='pipe_events_gaps'
                )

                port_visits >> features
                encounters >> features

                # Points to each independent event
                features >> events_anchorages
                features >> events_encounters
                features >> events_fishing
                features >> events_gaps

        return dag


for mode in ['daily','monthly', 'yearly']:
    dag_instance = VMSGenericDagFactory(schedule_interval='@{}'.format(mode)).build(mode)
    globals()[dag_instance.dag_id()] = dag_instance
