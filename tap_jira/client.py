"""REST client handling, including tap-jiraStream base class."""

from __future__ import annotations
import os

from pathlib import Path
from typing import Any, Callable, Iterable
import google.auth
import google.auth.transport.requests
import google.oauth2.id_token

import requests
from singer_sdk.authenticators import BasicAuthenticator, BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.streams import RESTStream

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

LOGGER = singer.get_logger()
class JiraStream(RESTStream):
    """tap-jira stream class."""

    next_page_token_jsonpath = (
        "$.paging.start"  # Or override `get_next_page_token`.  # noqa: S105
    )

    records_jsonpath = "$[*]"  # Or override `parse_response`.

    # Set this value or override `get_new_paginator`.
    next_page_token_jsonpath = "$.next_page"

    @property
    def url_base(self) -> str:
        """
        Returns base url
        """
        domain = self.config["domain"]
        base_url = "https://{}:443/rest/api/3".format(domain)
        return base_url

    @property
    def authenticator(self) -> _Auth:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        auth_type = self.config["auth"]["flow"]

        if auth_type == "oauth":
            refresh_token = self.config["auth"]["refresh_token"]
            client_id = self.config["auth"]["client_id"]
            client_secret = self.config["auth"]["client_secret"]

            LOGGER.info("Refreshing token")
            LOGGER.info("Old refresh token: %s", refresh_token)
            res = requests.post(
                "https://auth.atlassian.com/oauth/token",
                data={
                    "grant_type": "refresh_token",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "refresh_token": refresh_token,
                },
            )
            LOGGER.info("New refresh token: %s", res.json()["refresh_token"])
            access_token = res.json()["access_token"]

            DEF_SCHEDULER_URL = os.environ.get('DEF_SCHEDULER_URL')
            TAP_INTEGRATION_ID = os.environ.get('TAP_INTEGRATION_ID')
            if not DEF_SCHEDULER_URL:
                LOGGER.warn("DEF_SCHEDULER_URL not set. Tokens only persisted locally.")
            else:
                try:
                    creds, project = google.auth.default()
                    # creds.valid is False, and creds.token is None
                    # Need to refresh credentials to populate those
                    auth_req = google.auth.transport.requests.Request()
                    id_token = google.oauth2.id_token.fetch_id_token(auth_req, DEF_SCHEDULER_URL + '/')

                    LOGGER.info("Got id_token: {}".format(id_token))
                    url = DEF_SCHEDULER_URL + f'/v3/el/callback/update_integration_details/{TAP_INTEGRATION_ID}'
                    LOGGER.info(f"Persisting tokens to {url}")
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
                        LOGGER.warn(f"Failed to persist refresh token to Definite. Status code: {res.status_code}")
                except Exception as e:
                    LOGGER.warn(f"Failed to persist refresh token to Definite. Error: {e}")

            return BearerTokenAuthenticator.create_for_stream(
                self,
                token=access_token,
            )
        else:
            return BasicAuthenticator.create_for_stream(
                self,
                password=self.config["auth"]["password"],
                username=self.config["auth"]["username"],
            )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")  # noqa: ERA001
        return headers

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            params["startAt"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        return params

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: t.Any | None,
    ) -> t.Any | None:
        """Return a token for identifying next page or None if no more pages."""
        # If pagination is required, return a token which can be used to get the
        #       next page. If this is the final page, return "None" to end the
        #       pagination loop.
        resp_json = response.json()

        if previous_token is None:
            previous_token = 0

        total = -1
        results = 0
        _value = None
        is_last = None

        if isinstance(resp_json, dict):
            if resp_json.get(self.instance_name) is not None:
                _value = resp_json.get(self.instance_name)
                total = resp_json.get("total", -1)
                is_last = resp_json.get("isLast")
                results = len(_value)

        if type(is_last) == bool:
            if total == -1 and not is_last:
                return previous_token + results

        if _value is None:
            page = resp_json
            if len(page) == 0 or total <= previous_token + results:
                return None
        else:
            if len(_value) == 0 or total <= previous_token + results:
                return None
        return previous_token + results
