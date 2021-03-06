#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

hbase_classpath=`hbase classpath`
arr=(`echo $hbase_classpath | cut -d ":"  --output-delimiter=" " -f 1-`)
hbase_common_path=
for data in ${arr[@]}
do
    result=`echo $data | grep -e 'hbase-common[a-z0-9A-Z\.-]*jar' | grep -v tests`
    if [ $result ]
    then
        hbase_common_path=$data
    fi
done

if [ -z "$hbase_common_path" ]
then
    echo "hbase-common lib not found"
    exit 1
fi

hbase_dependency=${hbase_common_path}
echo "hbase dependency: $hbase_dependency"
export hbase_dependency