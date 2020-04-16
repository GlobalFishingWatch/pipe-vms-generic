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

There is also the need to configure at least one json object in Airflow Variables to put to work.

The configuration will require:

```json
{
  "dataflow_runner": "DataflowRunner",
  "docker_image": "gcr.io/world-fishing-827/github-globalfishingwatch-pipe-features:d34-8",
  "docker_run": "{{ var.value.DOCKER_RUN }}",
  "normalized_tables": "pipe_country_production_vYYYYYMMDD",
  "pipeline_bucket": "{{ var.value.PIPELINE_BUCKET }}",
  "pipeline_dataset": "{{ var.value.PIPELINE_DATASET }}",
  "project_id": "{{ var.value.PROJECT_ID }}",
  "source_dataset": "{{ var.value.PIPELINE_DATASET }}",
  "temp_bucket": "{{ var.value.TEMP_BUCKET }}"
}
```

