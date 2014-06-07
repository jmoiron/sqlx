#!/bin/bash

serve="python -m SimpleHTTPServer 8011 > /dev/null 2>&1"
reflex -r '.*\.(md|jinja)' -s -- sh -c "rt -t custom.jinja --prettify . && $serve"

