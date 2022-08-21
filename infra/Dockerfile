ARG APP_NAME=social_media_analysis
ARG APP_PATH=/opt/$APP_NAME
ARG APP_USER=koksang
ARG APP_MAINTAINER="$APP_USER <koksanggl@gmail.com>"
ARG BASE_IMAGE=python:3.9-slim
ARG POETRY_VERSION=1.1.14
ARG POETRY_HOME=/opt/poetry
ARG DBT_HOME="$APP_PATH/dbt"

# ----------------------------------------------------
# build image
# ----------------------------------------------------
FROM $BASE_IMAGE as build
ARG APP_NAME
ARG APP_PATH
ARG POETRY_VERSION
ARG POETRY_HOME

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    # poetry
    POETRY_VERSION=$POETRY_VERSION \
    POETRY_HOME=$POETRY_HOME \
    # poetry will create virtualenv in .venv in root project path $APP_PATH
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    PATH="$POETRY_HOME/bin:$PATH"

# Install Poetry - respects $POETRY_VERSION & $POETRY_HOME
ADD https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py install-poetry.py 
RUN python install-poetry.py

# Import project files & build application
WORKDIR $APP_PATH
COPY ./pyproject.toml ./poetry.lock ./
RUN poetry install --no-dev -vvv -n

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
    PATH="$APP_PATH/.venv/bin:$PATH"

# Get build artifact wheel and install it respecting dependency versions
WORKDIR $APP_PATH
COPY --from=build $APP_PATH/.venv ./.venv
COPY ./$APP_NAME ./$APP_NAME
COPY ./dbt $DBT_HOME

# Entrypoint script
COPY ./infra/docker-entrypoint.sh ./docker-entrypoint.sh
RUN chmod +x ./docker-entrypoint.sh
ENTRYPOINT ["./docker-entrypoint.sh"]