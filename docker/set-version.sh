#!/bin/sh

set -e

version="$1"

sed -i -e "s/^__version__ = .*/__version__ = \"${version}\"/g" bigquery_views_manager/__init__.py
