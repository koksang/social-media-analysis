#!/bin/sh

gcloud secrets versions access latest --secret=$GOOGLE_APPLICATION_CREDENTIALS_SECRET >> application_credentials.json

export GOOGLE_APPLICATION_CREDENTIALS="application_credentials.json"

export GOOGLE_PROJECT_ID=$(jq -r '.project_id' $GOOGLE_APPLICATION_CREDENTIALS)

exec "$@"