#!/usr/bin/env bash

PIPELINE='pipe_vms_generic'
PIPELINE_VERSION=$(python -c "import pkg_resources; print(pkg_resources.get_distribution('${PIPELINE}').version)")
