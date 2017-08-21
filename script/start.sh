#!/bin/sh
base_dir=$(dirname $0)
cd $base_dir
exec_dir=$(dirname $PWD)
cd ${exec_dir}
supervisord