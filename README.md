# pipe-vms-generic
Generic pipeline for all VMS integrations


## Description

The idea of the project is having all the common code that we need to use for VMS integrations.

It has the steps that a pipeline needs. But it is divided in two main parts.

The part that it is consider pre-processing data.
They are the steps you need to implement in case you are building a VMS pipeline, and they include:

- checking source validation
- fetching the data to be processed.


The part of the post-processing data.
The ordered list will be:

- segmenter pipeline
- measures pipeline
- encounters pipeline
- anchorages pipeline
- features pipeline
- events pipeline

The output will be the Airflow DAG to be processed.

## Configuration

It is a requirement to have the post-processing data pipelines installed in the same cluster where this pipeline is installed.

There is also the need to configure at least one json object in Airflow Variables to put to work under `vms_list` key.

The configuration will require:

* `events_dataset`: The dataset where to store the generated events.
* `name`: The name added as suffix of the pipeline name.
* `normalized_tables`: The tables that will be run the source existance and must be already normalized.
* `pipeline_bucket`: The gcs backet where the pipeline will store the data.
* `pipeline_dataset`: The dataset where to store the output of all the pipeline.
* `start_date`: The pipeline start date for the current vms country.

This is an example:
```json
{
  "dag_install_path": "/usr/local/airflow/dags/github-globalfishingwatch-pipe-vms-generic",
  "dataflow_runner": "DataflowRunner",
  "docker_image": "gcr.io/world-fishing-827/github-globalfishingwatch-pipe-vms-generic",
  "docker_run": "{{ var.value.DOCKER_RUN }}",
  "project_id": "{{ var.value.PROJECT_ID }}",
  "temp_bucket": "{{ var.value.TEMP_BUCKET }}",
  "vms_list": [
    {
      "events_dataset": "{{ var.value.EVENTS_DATASET }}",
      "name": "indo_test",
      "normalized_tables": "indo_vms_normalized_",
      "pipeline_bucket": "{{ var.value.PIPELINE_BUCKET }}",
      "pipeline_dataset": "{{ var.value.PIPELINE_DATASET }}",
      "start_date": "2014-01-01"
    }
  ]
}
```

