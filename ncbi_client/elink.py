from ncbi_client.base_client import NCBIBaseClient


class ELinkClient(NCBIBaseClient):
    """
    Client for interacting with the ELink utility from NCBI E-utilities.

    The ELink endpoint is used to find related records within the same database
    or across different NCBI databases (e.g., gene → protein, pubmed → pmc).

    It also supports pipeline workflows via the Entrez History server when using
    commands such as `neighbor_history`, allowing results to be passed directly
    to other utilities like EFetch or ESummary.

    Methods:
        find_links: Retrieve linked records between databases.

    Example:
        link = ELinkClient()
        xml = link.find_links(
            dbfrom="gene",
            db="protein",
            id="1234",
            cmd="neighbor_history"
        )
    """

    def find_links(
        self,
        dbfrom: str,
        db: str,
        id: str,
        cmd: str = "neighbor_history"
    ) -> str:
        """
        Retrieve linked UIDs between two NCBI databases.

        This method queries the ELink endpoint to identify relationships between
        records in different databases. Optionally, results can be stored in the
        Entrez History server for downstream processing.

        Args:
            dbfrom (str): Source database name (e.g., "gene", "pubmed").
            db (str): Target database name (e.g., "protein", "pmc").
            id (str): UID from the source database.
            cmd (str, optional):
                Command specifying behavior of the request.
                Common values:
                    - "neighbor": return linked IDs
                    - "neighbor_history": store results in Entrez History
                Defaults to "neighbor_history".

        Returns:
            str: XML response containing linked IDs or Entrez History references
                 (QueryKey and WebEnv) when using history mode.

        Raises:
            requests.HTTPError: If the HTTP request fails.
        """

        # Define parameters for the ELink request
        params = {
            "dbfrom": dbfrom,  # Source database
            "db": db,          # Target database
            "id": id,          # Input UID
            "cmd": cmd,        # Command mode (e.g., neighbor_history)
            "retmode": "xml"   # Response format
        }

        # Execute request via base client
        return self.request("elink.fcgi", params)