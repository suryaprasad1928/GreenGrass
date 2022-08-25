ARG TRT_IMAGE_VERSION=21.08
FROM nvcr.io/nvidia/tensorrt:${TRT_IMAGE_VERSION}-py3 as trt-base

ARG TRT_IMAGE_VERSION
ARG OPENCV_VERSION=4.1.1
ARG APP_DIR=/usr/src/app
ARG SCRIPT_DIR=/opt/tensorrt/python
ARG DEBIAN_FRONTEND=noninteractive

ENV HOME=${APP_DIR}
ENV TZ=Asia/Calcutta
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV OPENBLAS_MAIN_FREE=1
ENV OPENBLAS_NUM_THREADS=1
ENV NO_AT_BRIDGE=1

# Install OpenCV and FastMOT dependencies
RUN apt-get -y update && \
    apt-get install -y --no-install-recommends \
    wget unzip tzdata \
    build-essential cmake pkg-config \
    libgtk-3-dev libcanberra-gtk3-module \
    libjpeg-dev libpng-dev libtiff-dev \
    libavcodec-dev libavformat-dev libswscale-dev \
    libv4l-dev libxvidcore-dev libx264-dev \
    gfortran libatlas-base-dev \
    python3-dev \
    gstreamer1.0-tools \
    libgstreamer1.0-dev \
    libgstreamer-plugins-base1.0-dev \
    gstreamer1.0-libav \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-plugins-ugly \
    libtbb2 libtbb-dev libdc1394-22-dev && \
    pip install -U --no-cache-dir setuptools pip && \
    pip install --no-cache-dir numpy==1.18.0

# Build OpenCV
WORKDIR ${HOME}
RUN wget https://github.com/opencv/opencv/archive/${OPENCV_VERSION}.zip && \
    unzip ${OPENCV_VERSION}.zip && rm ${OPENCV_VERSION}.zip && \
    mv opencv-${OPENCV_VERSION} OpenCV && \
    wget https://github.com/opencv/opencv_contrib/archive/${OPENCV_VERSION}.zip && \
    unzip ${OPENCV_VERSION}.zip && rm ${OPENCV_VERSION}.zip && \
    mv opencv_contrib-${OPENCV_VERSION} OpenCV/opencv_contrib

# If you have issues with GStreamer, set -DWITH_GSTREAMER=OFF and -DWITH_FFMPEG=ON
WORKDIR ${HOME}/OpenCV/build
RUN cmake \
    -DCMAKE_BUILD_TYPE=RELEASE \
    -DCMAKE_INSTALL_PREFIX=/usr/local \
    -DOPENCV_EXTRA_MODULES_PATH=${HOME}/OpenCV/opencv_contrib/modules \
    -DINSTALL_PYTHON_EXAMPLES=ON \
    -DINSTALL_C_EXAMPLES=OFF \
    -DBUILD_opencv_python2=OFF \
    -DBUILD_TESTS=OFF \
    -DBUILD_PERF_TESTS=OFF \
    -DBUILD_EXAMPLES=ON \
    -DBUILD_PROTOBUF=OFF \
    -DENABLE_FAST_MATH=ON \
    -DWITH_TBB=ON \
    -DWITH_LIBV4L=ON \
    -DWITH_CUDA=OFF \
    -DWITH_GSTREAMER=ON \
    -DWITH_GSTREAMER_0_10=OFF \
    -DWITH_FFMPEG=OFF .. && \
    make -j$(nproc) && \
    make install && \
    ldconfig && \
    rm -rf ${HOME}/OpenCV && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get autoremove

# Install Python dependencies
WORKDIR ${APP_DIR}/gg-setup
COPY requirements.txt .

# Specify your GPU compute with --build-arg for CuPy (e.g. "arch=compute_75,code=sm_75")
ARG CUPY_NVCC_GENERATE_CODE

# TensorFlow < 2 is not supported in ubuntu 20.04
RUN if [[ -z ${CUPY_NVCC_GENERATE_CODE} ]]; then \
        echo "CUPY_NVCC_GENERATE_CODE not set, building CuPy for all architectures (slower)"; \
    fi && \
    if dpkg --compare-versions ${TRT_IMAGE_VERSION} ge 20.12; then \
        CUPY_NUM_BUILD_JOBS=$(nproc) pip install --no-cache-dir --ignore-installed -r <(grep -ivE "tensorflow" requirements.txt); \
    else \
        dpkg -i ${SCRIPT_DIR}/*-tf_*.deb && \
        CUPY_NUM_BUILD_JOBS=$(nproc) pip install --no-cache-dir --ignore-installed -r requirements.txt; \
    fi

FROM trt-base as gg-base

# setup aws cli
RUN cd /tmp && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install

# installing few libraries needed for greengrass python components
COPY requirements-gg.txt /tmp
RUN pip install --no-cache-dir -r /tmp/requirements-gg.txt

# installing stream manager dependencies
RUN pip install git+https://github.com/aws-greengrass/aws-greengrass-stream-manager-sdk-python.git

# installing JDK
RUN apt update && apt install -y software-properties-common sudo
RUN wget -O- https://apt.corretto.aws/corretto.key | apt-key add - && \
    add-apt-repository 'deb https://apt.corretto.aws stable main' && \
    apt-get update && \
    apt-get install -y java-1.8.0-amazon-corretto-jdk

# Setup jupyter lab 
RUN pip install --no-cache-dir jupyterlab


# Replace the args to lock to a specific version
ARG GREENGRASS_RELEASE_VERSION=2.4.0
ARG GREENGRASS_ZIP_FILE=greengrass-${GREENGRASS_RELEASE_VERSION}.zip
ARG GREENGRASS_RELEASE_URI=https://d2s8p88vqu9w66.cloudfront.net/releases/${GREENGRASS_ZIP_FILE}
ARG GREENGRASS_ZIP_SHA256=greengrass.zip.sha256

# Author
LABEL maintainer="AWS IoT Greengrass"
# Greengrass Version
LABEL greengrass-version=${GREENGRASS_RELEASE_VERSION}

# Set up Greengrass v2 execution parameters
# TINI_KILL_PROCESS_GROUP allows forwarding SIGTERM to all PIDs in the PID group so Greengrass can exit gracefully
ENV TINI_KILL_PROCESS_GROUP=1 \ 
    GGC_ROOT_PATH=/greengrass/v2 \
    PROVISION=false \
    AWS_REGION=us-east-1 \
    THING_NAME=default_thing_name \
    THING_GROUP_NAME=default_thing_group_name \
    TES_ROLE_NAME=default_tes_role_name \
    TES_ROLE_ALIAS_NAME=default_tes_role_alias_name \
    COMPONENT_DEFAULT_USER=default_component_user \
    DEPLOY_DEV_TOOLS=false \
    INIT_CONFIG=default_init_config \
    TRUSTED_PLUGIN=default_trusted_plugin_path \
    THING_POLICY_NAME=default_thing_policy_name
RUN env

# Entrypoint script to install and run Greengrass
COPY "greengrass-entrypoint.sh" /
COPY "${GREENGRASS_ZIP_SHA256}" /


# Install Greengrass v2 dependencies
RUN cd / && wget $GREENGRASS_RELEASE_URI && sha256sum -c ${GREENGRASS_ZIP_SHA256} && \
    chmod +x /greengrass-entrypoint.sh && \
    mkdir -p /opt/greengrassv2 $GGC_ROOT_PATH && unzip $GREENGRASS_ZIP_FILE -d /opt/greengrassv2 && rm $GREENGRASS_ZIP_FILE && rm $GREENGRASS_ZIP_SHA256

# modify /etc/sudoers
COPY "modify-sudoers.sh" /
RUN chmod +x /modify-sudoers.sh
RUN /modify-sudoers.sh

ENTRYPOINT ["/greengrass-entrypoint.sh"]


# ------------------------------------  Extras Below  ------------------------------------

# Stop the container (changes are kept)
# docker stop $(docker ps -ql)

# Start the container
# docker start -ai $(docker ps -ql)

# Delete the container
# docker rm $(docker ps -ql)

# Save changes before deleting the container
# docker commit $(docker ps -ql) fastmot:latest
