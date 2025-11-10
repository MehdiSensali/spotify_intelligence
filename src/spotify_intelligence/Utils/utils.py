import requests
import polars as pl


class MissingCredentialsError(Exception):
    """Custom exception for missing client ID or client secret."""

    pass


def get_bearer_token(client_id: str, client_secret: str) -> str:
    if not client_id or not client_secret:
        raise MissingCredentialsError(
            "Client ID and Client Secret must not be null or empty."
        )
    token_url = "https://accounts.spotify.com/api/token"
    token_data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    token_headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(url=token_url, data=token_data, headers=token_headers)
    status_code = response.status_code
    if status_code == 200:
        return response.json()["access_token"]

    else:
        response.raise_for_status()
