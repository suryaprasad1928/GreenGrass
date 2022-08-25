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

# TODO : include exception capture
# TODO : Add logger
# TODO : Add process to check to avoid multiple create component process
# TODO : As of now, it supports only one artifact and compressed mode. Support multiple artifacts


# Base variables
S3_BUCKET="store-analytics-image-upload"
S3_PREFIX="Artifacts"
#ART_LOC="$HOME/GreengrassCore/gg-edge-inference-components"
ART_LOC="artifacts"
REC_LOC="recipes"
CORE_DEVICE_NAME="jetson-nx"
COMP_BASE_ARN="arn:aws:greengrass:ap-south-1:752061789774:"

# Helper functions
# Help output
function usage {
  echo
  echo "This is a Script to Launch create or recreate component version"
  echo "Usage: ./create-component.sh --comp-name <name of the component> --comp-version <version of component> --art-path <exisiting s3 artifact location> --prefix-path <archive prefix to be added>"
  echo "--art-path is optional. if we give this path, this script will not compress and upload, it will just use that to create/recreate the component"
  echo "Example: ./create-component.sh --comp-name com.metrics.collect --comp-version 1.0.0 --prefix-path fmot"
  echo
}

# Get input parameters
while [[ "$1" != "" ]]; do
  case $1 in
      -c | --comp-name )       	shift
                              COMP_NAME=$1
                              ;;
      -v | --comp-version )  	shift
                              COMP_VERSION=$1
                              ;;
	  -a | --art-path )			shift
							  ART_PATH=$1	
							  ;;
	  -p | --prefix-path )		shift
							  PREFIX_PATH=$1	
							  ;;
	  -r | --recreate )		shift
							  RECREATE=$1	
							  ;;
      -h | --help )           usage
                              exit
                              ;;
      * )                     usage
                              exit 1
  esac
  shift
done

if [ -z ${COMP_NAME+x} ]; then
    echo "Please Specify component name"
    usage
    exit 1
fi

if [ -z ${COMP_VERSION+x} ]; then
    echo "Please Specify component version"
    usage
    exit 1
fi

if [ -z ${PREFIX_PATH+x} ]; then
    echo "Specify prefix to be used by the compressed file"
    usage
    exit 1
fi

if [[ -n ${ART_PATH} ]]; then
    echo "Artifact path has been given. it will not compress and upload to s3"
    echo "It will use the given artifact path"
else
    echo "compressing and uploading files from $ART_LOC/artifacts/${COMP_NAME}/${COMP_VERSION} with the prefix ${PREFIX_PATH} to the s3 bucket ${S3_BUCKET}"
    echo "It will use "
fi

echo "Creating component : $COMP_NAME , Version : $COMP_VERSION"

COMP_ARN="${COMP_BASE_ARN}components:$COMP_NAME:versions:$COMP_VERSION"
EXISTING_OUTPUT=$(aws greengrassv2 describe-component --arn ${COMP_ARN})
EXISTING_RESULT=$?

if [[ $EXISTING_RESULT == 254 ]];then
    echo "The component is not existing. It will be created now."
    CREATE=1
else
    echo "The component is existing. The details are  : $EXISTING_OUTPUT "
    if [[ $RECREATE == "TRUE" ]]; then
        echo "Recreate option is selected.The component will be deleted and recreated"
        DELETE=1
        CREATE=1
    else
        echo "Recreate option was not selected. Not doing anything. Exiting "
        exit 2
    fi
fi

if [[ ! -n $ART_PATH ]];then
    echo "Artifact path has not given. Compress from the artifacts folder and upload to given s3 bucket"
    COMP_LOC=${ART_LOC}/$COMP_NAME/$COMP_VERSION
    
    if [[ ! -d $COMP_LOC ]]; then
        echo "The artifacts folder ($COMP_LOC) for given component does not exist. Exiting"
        exit 3
    fi
    cd $COMP_LOC
    zip -r ./${PREFIX_PATH}.zip ./* -x */__pycache__/\* -x "*.trt" -x "*.zip"
    if [[ $? == 0 ]]; then
        echo "Compression completed. Uploading to s3 now"
        S3_FINAL_PATH="s3://${S3_BUCKET}/${S3_PREFIX}/$COMP_NAME/$COMP_VERSION/${PREFIX_PATH}.zip"
        aws s3 cp ./${PREFIX_PATH}.zip $S3_FINAL_PATH
        if [[ $?==0 ]];then
            echo "The upload to s3 completed successfully with this path $S3_FINAL_PATH. Moving on to creating component"
            rm ${PREFIX_PATH}.zip
            cd -
            UPDATE_ART_PATH=$S3_FINAL_PATH
        else
            echo "Failed to upload to s3. Check for errors in the log. exiting"
            exit 4
        fi
    else   
        echo "Compression failed. check for errors in the log. Exiting"
        exit 2
    fi
else
    echo "The artifact path given as ($ART_PATH). Checking if that path exists in s3"
    aws s3 ls ${ART_PATH}
    S3CHECK_RESULT=$?
    
    if [[ $S3CHECK_RESULT == 0 ]]; then
        echo "The given artifact s3 path is a valid path. Proceeding"
        UPDATE_ART_PATH=$ART_PATH
    else
        echo "The given artifact s3 path is not a valid location. Please check. Exiting"
        exit 4
    fi
fi


if [[ $DELETE == 1 ]]; then
    echo "Deleting the existing component. $COMP_NAME:$COMP_VERSION "
    DELETE_OUTPUT=$(aws greengrassv2 delete-component --arn $COMP_ARN)
    DELETE_RESULT=$?

    if [[ $DELETE_RESULT == 0 ]]; then
        echo "Delete of the component ($COMP_NAME:$COMP_VERSION) with arn ($COMP_ARN) is successful."
    else
        echo "Delete of the component ($COMP_NAME:$COMP_VERSION) with arn ($COMP_ARN) is failed. Check for errors in the log. Exiting"
        exit 3
    fi
fi

if [[ $CREATE == 1 ]]; then
    RECIPE_TEMPLATE="${REC_LOC}/$COMP_NAME.json"
    if [[ ! -e $RECIPE_TEMPLATE ]]; then
        echo "The recipe template file is missing. Expected location ($RECIPE_TEMPLATE). Please create it. exiting"
        exit 6
    fi
    RECIPE_VERSION="${REC_LOC}/$COMP_NAME-$COMP_VERSION.json"
    echo "Creating version specific file - ($RECIPE_VERSION)"
    cp ${RECIPE_TEMPLATE} ${RECIPE_VERSION}
    if [[ ! $? == 0 ]]; then\
        echo "Generating version specific file failed. check for errors in the log. Exiting"
        exit 7
    fi

    sed -i "s/#COMP_NAME#/$COMP_NAME/g" ${RECIPE_VERSION}
    sed -i "s/#COMP_VERSION#/$COMP_VERSION/g" ${RECIPE_VERSION}
    sed -i "s+#UPDATE_ART_PATH#+$UPDATE_ART_PATH+g" ${RECIPE_VERSION}
    sed -i "s/#PREFIX_PATH#/$PREFIX_PATH/g" ${RECIPE_VERSION}

    echo "updation of file with version specific values completed"
    echo $RECIPE_VERSION
    echo "Starting to create version in the cloud"
    CREATE_OUTPUT=$(aws greengrassv2 create-component-version --inline-recipe fileb://$RECIPE_VERSION)
    CREATE_RESULT=$?

    if [[ $CREATE_RESULT == 0 ]]; then
        echo $CREATE_OUTPUT
        echo "Create is successful. waiting for the component to be in deployable status."
        sleep 5
        #TODO :  fetch the component arn from the $CREATE_VALUE , create a loop to check till the component status reaches deployable
    else 
        echo "Component creation failed. Check for errors in the log. Exiting"
        exit 9
    fi
fi

echo "Component creation process completed"