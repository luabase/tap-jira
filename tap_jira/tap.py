"""tap-jira tap class."""

from __future__ import annotations
import logging
import os

import google.auth
import google.auth.transport.requests
import google.oauth2.id_token
import requests
from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_jira import streams


class TapJira(Tap):
    """tap-jira tap class."""

    name = "tap-jira"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Earliest record date to sync",
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            description="Latest record date to sync",
        ),
        th.Property(
            "domain",
            th.StringType,
            description="Site URL",
            required=True,
        ),
        th.Property(
            "auth",
            th.DiscriminatedUnion(
                "flow",
                oauth=th.ObjectType(
                    th.Property(
                        "refresh_token", th.StringType, required=True, secret=True
                    ),
                    th.Property(
                        "client_id", th.StringType, required=True
                    ),
                    th.Property(
                        "client_secret", th.StringType, required=True, secret=True
                    ),
                    additional_properties=False,
                ),
                password=th.ObjectType(
                    th.Property("username", th.StringType, required=True),
                    th.Property("password", th.StringType, required=True, secret=True),
                    additional_properties=False,
                ),
            ),
            required=True,
        ),
        th.Property(
            "page_size",
            th.ObjectType(
                th.Property("issues", th.IntegerType, description="Page size for issues stream", default=100),
            ),
        ),
    ).to_dict()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # refresh access token if using oauth
        if self.config["auth"]["flow"] == "oauth":
            refresh_token = self.config["auth"]["refresh_token"]
            client_id = self.config["auth"]["client_id"]
            client_secret = self.config["auth"]["client_secret"]

            logging.info("Refreshing token")
            logging.info("Old refresh token: %s", refresh_token)
            res = requests.post(
                "https://auth.atlassian.com/oauth/token",
                data={
                    "grant_type": "refresh_token",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "refresh_token": refresh_token,
                },
            )
            logging.info("New access token: %s", res.json()["access_token"])
            logging.info("New refresh token: %s", res.json()["refresh_token"])
            self.config["auth"]["refresh_token"] = res.json()["refresh_token"]
            self.config["auth"]["access_token"] = res.json()["access_token"]

            DEF_SCHEDULER_URL = os.environ.get('DEF_SCHEDULER_URL')
            TAP_INTEGRATION_ID = os.environ.get('TAP_INTEGRATION_ID')
            if not DEF_SCHEDULER_URL:
                logging.warn("DEF_SCHEDULER_URL not set. Tokens only persisted locally.")
            else:
                try:
                    creds, project = google.auth.default()
                    # creds.valid is False, and creds.token is None
                    # Need to refresh credentials to populate those
                    auth_req = google.auth.transport.requests.Request()
                    id_token = google.oauth2.id_token.fetch_id_token(auth_req, DEF_SCHEDULER_URL + '/')

                    url = DEF_SCHEDULER_URL + f'/v3/el/callback/update_integration_details/{TAP_INTEGRATION_ID}'
                    logging.info(f"Persisting tokens to {url}")
                    res = requests.post(
                        url,
                        headers={
                            'Authorization': f'Bearer {id_token}',
                            'Content-Type': 'application/json'
                        },
                        json={
                            'refreshToken': self.refresh_token
                        }
                    )
                    if res.status_code != 200:
                        logging.warn(f"Failed to persist refresh token to Definite. Status code: {res.status_code}")
                except Exception as e:
                    logging.warn(f"Failed to persist refresh token to Definite. Error: {e}")



    def discover_streams(self) -> list[streams.JiraStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.UsersStream(self),
            streams.FieldStream(self),
            streams.ServerInfoStream(self),
            streams.IssueTypeStream(self),
            streams.ProjectStream(self),
            streams.WorkflowStatusStream(self),
            streams.IssueStream(self),
            streams.PermissionStream(self),
            streams.ProjectRoleStream(self),
            streams.PriorityStream(self),
            streams.PermissionHolderStream(self),
            streams.SprintStream(self),
            streams.ProjectRoleActorStream(self),
            streams.AuditingStream(self),
            streams.DashboardStream(self),
            streams.FilterSearchStream(self),
            streams.FilterDefaultShareScopeStream(self),
            streams.GroupsPickerStream(self),
            streams.LicenseStream(self),
            streams.ScreensStream(self),
            streams.ScreenSchemesStream(self),
            streams.StatusesSearchStream(self),
            streams.WorkflowStream(self),
            streams.WorkflowSearchStream(self),
            streams.Resolutions(self),
            streams.IssueChangeLogStream(self),
            streams.IssueComments(self),
            streams.BoardStream(self),
            streams.IssueWatchersStream(self),
            streams.IssueWorklogs(self),
        ]


if __name__ == "__main__":
    TapJira.cli()
