ARG APP_NAME=social_media_analysis
ARG APP_PATH=/opt/$APP_NAME
ARG APP_USER=koksang
ARG APP_MAINTAINER="$APP_USER <koksanggl@gmail.com>"
ARG BASE_IMAGE=python:3.9-slim
ARG POETRY_VERSION=1.1.14
ARG POETRY_HOME=/opt/poetry
ARG DBT_HOME="$APP_PATH/dbt"
ARG CLOUD_SDK_VERSION=398.0.0

# ----------------------------------------------------
# build image
# ----------------------------------------------------
FROM $BASE_IMAGE as build
ARG APP_NAME \
    APP_PATH \
    POETRY_VERSION \
    POETRY_HOME \
    CLOUD_SDK_VERSION

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    # poetry
    POETRY_VERSION=$POETRY_VERSION \
    POETRY_HOME=$POETRY_HOME \
    # poetry will create virtualenv in .venv in root project path $APP_PATH
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    PATH="$POETRY_HOME/bin:$PATH" \
    # gcloud
    CLOUD_SDK_VERSION=$CLOUD_SDK_VERSION

# Install Poetry - respects $POETRY_VERSION & $POETRY_HOME
ADD https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py install-poetry.py 
RUN python install-poetry.py

# Import project files & build application
WORKDIR $APP_PATH
COPY ./pyproject.toml ./poetry.lock ./
RUN poetry install --no-dev -vvv -n --remove-untracked

# Install gcloud & gsutil
ARG INSTALL_COMPONENTS
RUN apt update -qqy && apt install -qqy \
    curl \
    apt-transport-https \
    lsb-release \
    git \
    gnupg && \
    pip3 install -U crcmod && \
    export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" && \
    echo "deb https://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" > /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
    apt update && apt install -y google-cloud-sdk=${CLOUD_SDK_VERSION}-0 $INSTALL_COMPONENTS && \
    gcloud --version

# ----------------------------------------------------
# final image
# ----------------------------------------------------
FROM $BASE_IMAGE as final
ARG APP_NAME
ARG APP_PATH
ARG APP_USER
ARG APP_MAINTAINER
ARG DBT_HOME

LABEL org.opencontainers.image.authors=$APP_MAINTAINER
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    # set app env variables
    APP_PATH=$APP_PATH \
    APP_NAME=$APP_NAME \
    # set pythonpath
    PATH="$APP_PATH/.venv/bin:$PATH" \
    PATH="/usr/lib/google-cloud-sdk/bin/:$PATH:/opt/google-cloud-sdk/bin/"

# Get build artifact wheel and install it respecting dependency versions
WORKDIR $APP_PATH
COPY --from=build $APP_PATH/.venv ./.venv
COPY --from=build /usr/lib/google-cloud-sdk /usr/lib/google-cloud-sdk
COPY ./$APP_NAME ./$APP_NAME
COPY ./dbt $DBT_HOME

# Entrypoint script
COPY ./infra/docker-entrypoint.sh ./docker-entrypoint.sh
RUN mkdir -p /usr/share/man/man1/ && \
    groupadd -r -g 1000 cloudsdk && \
    useradd -r -u 1000 -m -s /bin/bash -g cloudsdk cloudsdk && \
    gcloud config set core/disable_usage_reporting true && \
    gcloud config set component_manager/disable_update_check true && \
    gcloud config set metrics/environment github_docker_image && \
    chmod +x ./docker-entrypoint.sh
ENTRYPOINT ["./docker-entrypoint.sh"]