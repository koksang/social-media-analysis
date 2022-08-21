"""Extended operators with credentials
"""

import os
from typing import Union
from pathlib import Path
from airflow.providers.docker.operators.docker import DockerOperator

GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", None)


class GcloudDockerOperator(DockerOperator):
    def __init__(
        self,
        google_application_credentials: Union[
            str, None
        ] = GOOGLE_APPLICATION_CREDENTIALS,
        **kwargs
    ):
        check_google_application_credentials()
        if google_application_credentials:
            environment = kwargs.get("environment", {})
            environment.update(
                {"GOOGLE_APPLICATION_CREDENTIALS": google_application_credentials}
            )
            kwargs["environment"] = environment
        super().__init__(**kwargs)


def check_google_application_credentials(
    env_key: str = "GOOGLE_APPLICATION_CREDENTIALS",
    file_name: str = "google_application_credentials.json",
    credentials_base_path: str = "credentials",
):
    """To check if filename `google_application_credentials.json` exists in credentials base folder 'credentials'.
    If exists, make it as the env value of $GOOGLE_APPLICATION_CREDENTIALS.

    :param str env_key: _description_, defaults to "GOOGLE_APPLICATION_CREDENTIALS"
    :param str file_name: _description_, defaults to "google_application_credentials.json"
    :param str credentials_base_path: _description_, defaults to "credentials"
    """
    google_application_credentials_json = Path(credentials_base_path, file_name)
    if google_application_credentials_json.exists():
        os.environ[env_key] = str(google_application_credentials_json)
