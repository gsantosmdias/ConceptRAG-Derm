from ncbi_client.base_client import NCBIBaseClient
from typing import List, Optional, Union


class EFetchClient(NCBIBaseClient):
    """
    Client for interacting with the EFetch utility from NCBI E-utilities.

    The EFetch endpoint is used to retrieve full records (e.g., abstracts,
    sequences, or full-text metadata) using either:
        - A list of UIDs (IDs), or
        - Entrez History server parameters (query_key + WebEnv)

    It supports multiple output formats controlled via `retmode` and `rettype`.

    Notes:
        - There is no strict maximum for the number of UIDs, but for requests
          exceeding ~200 UIDs, NCBI recommends using HTTP POST instead of GET.
        - This implementation currently uses GET requests.

    Methods:
        fetch_records: Retrieve full records using IDs or Entrez history.

    Example:
        fetch = EFetchClient()
        txt = fetch.fetch_records(
            db="pubmed",
            query_key="1",
            webenv="...",
            retmode="xml"
        )
    """

    def fetch_records(
        self,
        db: str,
        query_key: str,
        webenv: str,
        retmode: str = "xml",
        retstart: int = 0,
        retmax: int = 10000,
        id: Optional[Union[int, List[int]]] = None
    ) -> str:
        """
        Download full data records from NCBI using the EFetch endpoint.

        This method retrieves records using either:
            - Entrez History (query_key + WebEnv), or
            - A direct list of UIDs (id parameter)

        Args:
            db (str): Target NCBI database (e.g., "pubmed", "pmc", "nucleotide").
            query_key (str): QueryKey returned by a prior ESearch request.
            webenv (str): WebEnv session token returned by ESearch.
            retmode (str, optional): Output format ("xml", "text", etc.). Defaults to "xml".
            retstart (int, optional): Starting index for pagination. Defaults to 0.
            retmax (int, optional): Maximum number of records to retrieve. Defaults to 10000.
            id (Optional[Union[int, List[int]]], optional):
                One or more UIDs. Can be a single integer or a list of integers.

        Returns:
            str: Raw response content (XML or text depending on `retmode`).

        Raises:
            requests.HTTPError: If the HTTP request fails.
        """

        # Construct query parameters for the EFetch request
        params = {
            "id": id,                  # Optional UID(s)
            "db": db,                 # Target database
            "query_key": query_key,   # History server query key
            "WebEnv": webenv,         # History server environment token
            "retmode": retmode,       # Output format
            "retstart": retstart,     # Pagination start index
            "retmax": retmax          # Number of records to retrieve
        }

        # Delegate request execution to the base client
        return self.request("efetch.fcgi", params)