from ncbi_client.base_client import NCBIBaseClient
import xml.etree.ElementTree as ET
from typing import List, Dict, Any


class ESummaryClient(NCBIBaseClient):
    """
    Client for interacting with the ESummary utility from NCBI E-utilities.

    The ESummary endpoint retrieves concise metadata summaries (DocSums)
    for a set of UIDs. It is typically used as a lightweight alternative
    to EFetch when only structured metadata is required.

    Methods:
        fetch_summary: Retrieve and parse document summaries using Entrez History.
        parse_esummary_xml: Convert raw XML response into structured Python objects.

    Example:
        summary = ESummaryClient()
        result = summary.fetch_summary(
            db="pubmed",
            query_key="1",
            webenv="..."
        )
    """

    def parse_esummary_xml(self, xml_str: str) -> Dict[str, Any]:
        """
        Parse an NCBI ESummary XML response into a structured dictionary.

        This method extracts each <DocSum> entry and converts it into a
        dictionary, handling both flat fields and nested list structures.

        Args:
            xml_str (str): Raw XML string returned by ESummary.

        Returns:
            Dict[str, Any]:
                A dictionary containing:
                    - "raw" (str): Original XML response
                    - "results" (List[Dict]): Parsed document summaries
        """

        # Parse XML string into an ElementTree root
        root: ET.Element = ET.fromstring(xml_str)

        # Container for parsed document summaries
        results: List[Dict[str, Any]] = []

        # Iterate over each document summary (DocSum)
        for doc in root.findall('DocSum'):
            # Initialize dictionary with document ID
            doc_data: Dict[str, Any] = {'Id': doc.findtext('Id')}

            # Iterate over all metadata fields (Item elements)
            for item in doc.findall('Item'):
                name: str = item.attrib.get('Name')  # Field name
                type_: str = item.attrib.get('Type')  # Field type

                if type_ == 'List':
                    # Handle nested list structures (e.g., authors, identifiers)
                    values: List[Dict[str, Any]] = []

                    for subitem in item.findall('Item'):
                        subname: str = subitem.attrib.get('Name')
                        subval: str = subitem.text
                        values.append({subname: subval})

                    doc_data[name] = values
                else:
                    # Handle simple text fields
                    doc_data[name] = item.text

            # Append parsed document to results list
            results.append(doc_data)

        # Return structured output including raw XML
        parsed_dict: Dict[str, Any] = {"raw": xml_str, "results": results}
        return parsed_dict

    def fetch_summary(
        self,
        db: str,
        query_key: str,
        webenv: str,
        retstart: int = 0,
        retmax: int = 10000,
        retmode: str = "xml",
    ) -> Dict[str, Any]:
        """
        Retrieve and parse document summaries from NCBI using ESummary.

        This method queries the ESummary endpoint using Entrez History
        parameters (query_key + WebEnv), then parses the XML response into
        a structured Python dictionary.

        Args:
            db (str): Target database name (e.g., "pubmed").
            query_key (str): QueryKey returned by a prior ESearch call.
            webenv (str): WebEnv token returned by ESearch.
            retstart (int, optional): Starting index for pagination. Defaults to 0.
            retmax (int, optional): Maximum number of records to retrieve. Defaults to 10000.
            retmode (str, optional): Response format (typically "xml"). Defaults to "xml".

        Returns:
            Dict[str, Any]:
                Parsed response containing:
                    - "raw": original XML string
                    - "results": list of document summaries

        Raises:
            requests.HTTPError: If the HTTP request fails.
            xml.etree.ElementTree.ParseError: If XML parsing fails.
        """

        # Define parameters for the ESummary request
        params = {
            "db": db,                 # Target database
            "query_key": query_key,   # History server query key
            "WebEnv": webenv,         # History server environment token
            "retstart": retstart,     # Pagination start index
            "retmax": retmax,         # Number of records to retrieve
            "retmode": retmode        # Response format
        }

        # Execute request via base client
        xml_response: str = self.request("esummary.fcgi", params)

        # Parse XML response into structured dictionary
        parsed_response: Dict[str, Any] = self.parse_esummary_xml(xml_response)

        return parsed_response