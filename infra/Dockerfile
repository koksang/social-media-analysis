ARG APP_NAME=social-media-analysis
ARG APP_PATH=/opt/$APP_NAME
ARG APP_USER=koksang
ARG PYTHON_VERSION=3.9-slim
ARG POETRY_VERSION=1.1.13

# ----------------------------------------------------
# staging
# ----------------------------------------------------
FROM python:$PYTHON_VERSION as staging
ARG APP_NAME
ARG APP_PATH
ARG POETRY_VERSION

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    POETRY_VERSION=$POETRY_VERSION \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1

# Install Poetry - respects $POETRY_VERSION & $POETRY_HOME
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py | python
ENV PATH="$POETRY_HOME/bin:$PATH"

# Import our project files
WORKDIR $APP_PATH
COPY ./pyproject.toml ./
COPY ./src ./src

# ----------------------------------------------------
# development
# ----------------------------------------------------
FROM staging as development
ARG APP_NAME
ARG APP_PATH

# Install project in editable mode and with development dependencies
WORKDIR $APP_PATH
RUN poetry install

# In development mode we use the default flask webserver
ENV FLASK_APP=$APP_NAME \
    FLASK_ENV=development \
    FLASK_RUN_HOST=0.0.0.0 \
    FLASK_RUN_PORT=8888

ENTRYPOINT ["poetry", "run"]
CMD ["flask", "run"]

# ----------------------------------------------------
# build
# ----------------------------------------------------
FROM staging as build
ARG APP_PATH

WORKDIR $APP_PATH
RUN poetry build --format wheel
RUN poetry export --format requirements.txt --output constraints.txt --without-hashes

# ----------------------------------------------------
# production
# ----------------------------------------------------
FROM python:$PYTHON_VERSION as production
ARG APP_NAME
ARG APP_PATH

LABEL org.opencontainers.image.authors="koksang <koksanggl@gmail.com>"
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100

# Get build artifact wheel and install it respecting dependency versions
WORKDIR $APP_PATH
COPY --from=build $APP_PATH/dist/*.whl ./
COPY --from=build $APP_PATH/constraints.txt ./
RUN pip install ./$APP_NAME*.whl --constraint constraints.txt

# gunicorn port. Naming is consistent with GCP Cloud Run
ENV PORT=8888
# export APP_NAME as environment variable for the CMD
ENV APP_NAME=$APP_NAME

# Entrypoint script
COPY ./docker/docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["gunicorn", "--bind :$PORT", "--workers 1", "--threads 1", "--timeout 0", "\"$APP_NAME:create_app()\""]