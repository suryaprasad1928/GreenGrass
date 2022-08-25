#!/bin/bash
# /*********************************************************************************************************************
# *  Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           *
# *                                                                                                                    *
# *  Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance        *
# *  with the License. A copy of the License is located at                                                             *
# *                                                                                                                    *
# *      http://aws.amazon.com/asl/                                                                                    *
# *                                                                                                                    *
# *  or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES *
# *  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    *
# *  and limitations under the License.                                                                                *
# ******************************************************************************************************************** */


deployment_file=$1

echo "Greengrass deployment started "

if [[ -e "${deployment_file}" ]]
then
    aws greengrassv2 create-deployment --cli-input-json file://"${deployment_file}"
else
    echo "The deploymnet json configuration file (${deployment_file}) does not exist. Exiting"
    exit 1
fi

echo "Deployment completed !"