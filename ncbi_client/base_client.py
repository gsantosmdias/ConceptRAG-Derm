import requests
from typing import Dict


# https://www.ncbi.nlm.nih.gov/books/NBK25499/
class NCBIBaseClient:
    """
    Base client for interacting with the NCBI E-utilities API.

    This class provides a reusable interface for making HTTP GET requests to
    the NCBI Entrez Programming Utilities (E-utilities). It centralizes the
    base URL and default parameters required by NCBI (such as tool name and email).

    Notes:
        - NCBI recommends including a valid email and tool name in requests
          for identification and usage tracking.
        - This class is intended to be subclassed for specific endpoints
          (e.g., ESearch, EFetch, ESummary).

    Attributes:
        BASE_URL (str): Root URL for all E-utilities endpoints.
        default_params (Dict[str, str]): Default query parameters included in every request.

    Example:
        class ESearchClient(NCBIBaseClient):
            def search(self, term: str):
                return self.request("esearch.fcgi", {"db": "pubmed", "term": term})
    """

    BASE_URL: str = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"

    def __init__(self, tool: str = "my_tool", email: str = "example@example.com") -> None:
        """
        Initialize the base client with default parameters.

        Args:
            tool (str): Name of the tool accessing the API (recommended by NCBI).
            email (str): Contact email (recommended by NCBI for API usage).
        """
        # Default parameters appended to every request
        self.default_params: Dict[str, str] = {
            "tool": tool,
            "email": email,
        }

    def request(self, endpoint: str, params: Dict[str, str]) -> str:
        """
        Send a GET request to a specific E-utilities endpoint.

        This method merges default parameters (tool, email) with custom query parameters,
        constructs the full request URL, and performs the HTTP request.

        Args:
            endpoint (str): API endpoint (e.g., "esearch.fcgi", "efetch.fcgi").
            params (Dict[str, str]): Query parameters specific to the request.

        Returns:
            str: Raw response content as a string (typically XML or JSON).

        Raises:
            requests.HTTPError: If the HTTP request fails (non-2xx status code).
        """
        # Construct full URL for the endpoint
        url: str = self.BASE_URL + endpoint

        # Merge default parameters with request-specific parameters
        final_params: Dict[str, str] = {**self.default_params, **params}

        # Perform HTTP GET request
        response: requests.Response = requests.get(url, params=final_params)

        # Raise exception if request failed
        response.raise_for_status()

        # Return raw response text
        return response.text