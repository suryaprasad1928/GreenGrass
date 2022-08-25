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

comp_name=$1
comp_version=$2
deployment_file=$3
art_location="artifacts"
rec_location="recipes/local"

echo "Greengrass local deployment started "

if [[ -n "${comp_name}" ]];then

    echo "Removing the existing deployed component, if it exists"
    sudo /greengrass/v2/bin/greengrass-cli deployment create --remove $comp_name

    if [[ -e "${deployment_file}" ]];
    then
        echo "Local deployment submitted with the given deployment configuration"
        sudo /greengrass/v2/bin/greengrass-cli deployment create \
            --recipeDir $rec_location \
            --artifactDir $art_location \
            --merge "${comp_name}=${comp_version}" \
            --update-config $deployment_file
    else
        echo "Local deployment submitted using the default configuration. no config overrides"
        sudo /greengrass/v2/bin/greengrass-cli deployment create \
            --recipeDir $rec_location \
            --artifactDir $art_location \
            --merge "$comp_name=$comp_version"
    fi

    echo "Waiting for deployment to be initiated..."
    sleep 5
    echo "Listing the currently listed components"
    sudo /greengrass/v2/bin/greengrass-cli component list    
else
    echo "The component name has not been given. Aborting"
    exit 2
fi


echo "Deployment completed !"
