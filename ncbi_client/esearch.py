from ncbi_client.base_client import NCBIBaseClient
from typing import Dict, Any
import xml.etree.ElementTree as ET


class ESearchClient(NCBIBaseClient):
    """
    Client for interacting with the ESearch utility from NCBI E-utilities.

    The ESearch endpoint is used to query an NCBI database using an Entrez-formatted
    search term and retrieve matching record identifiers (UIDs).

    It also supports integration with the Entrez History server, allowing results
    to be stored and reused in subsequent requests (e.g., EFetch, ESummary).

    Methods:
        run_query: Execute a search query and return structured results.

    Example:
        search = ESearchClient()
        result = search.run_query(
            db="pubmed",
            term="cancer[MeSH Major Topic]"
        )
    """

    def run_query(
        self,
        db: str,
        term: str,
        retstart: int = 0,
        retmax: int = 10000,
        usehistory: bool = True
    ) -> Dict[str, Any]:
        """
        Execute a search query against an NCBI database using ESearch.

        This method retrieves matching UIDs for a given query and optionally
        stores them in the Entrez History server for downstream processing.

        Args:
            db (str): Target database name (e.g., "pubmed", "gene").
            term (str): Search query string in Entrez format.
            retstart (int, optional):
                Index of the first record to return (pagination offset).
                Defaults to 0.
            retmax (int, optional):
                Maximum number of UIDs to return. Defaults to 10000.
            usehistory (bool, optional):
                Whether to store results in the Entrez History server.
                If True, enables use of WebEnv and QueryKey in subsequent calls.
                Defaults to True.

        Returns:
            Dict[str, Any]: A dictionary containing:
                - "raw_response" (str): Full XML response as string
                - "count" (int): Total number of matching records
                - "webenv" (str): WebEnv session token (if usehistory=True)
                - "query_key" (str): QueryKey identifier (if usehistory=True)
                - "ids" (List[str]): List of retrieved UIDs

        Raises:
            requests.HTTPError: If the HTTP request fails.
            xml.etree.ElementTree.ParseError: If XML parsing fails.
        """

        # Define parameters for the ESearch request
        params = {
            "db": db,                             # Target database
            "term": term,                         # Entrez query string
            "retstart": retstart,                 # Pagination start index
            "retmax": retmax,                     # Maximum number of results
            "usehistory": "y" if usehistory else "n",  # Enable history server
            "retmode": "xml"                      # Response format
        }

        # Execute request via base client
        xml_response: str = self.request("esearch.fcgi", params)

        # Parse XML response into an ElementTree
        root: ET.Element = ET.fromstring(xml_response)

        # Extract relevant fields from the XML structure
        return {
            "raw_response": xml_response,  # Full XML response
            "count": int(root.findtext("Count")),  # Total number of results
            "webenv": root.findtext("WebEnv"),  # History server environment
            "query_key": root.findtext("QueryKey"),  # Query identifier
            "ids": [id_tag.text for id_tag in root.findall(".//Id")]  # List of UIDs
        }