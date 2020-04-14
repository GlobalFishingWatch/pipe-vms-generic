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
