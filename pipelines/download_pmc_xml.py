from ncbi_client.esearch import ESearchClient
from ncbi_client.efetch import EFetchClient
from tqdm import tqdm
import time
import os
from requests.exceptions import HTTPError, ChunkedEncodingError, ConnectionError
from typing import List, Dict, Any
import argparse
import logging
from pathlib import Path


def batch_process(
    query: str,
    total_articles: int,
    batch_size: int,
    output_dir_path: str,
    client_email: str = "",
    client_tool: str = "",
    initial_count: int = 0,
) -> None:
    """
    Download PMC articles in batches and save each article as an XML file.

    This function:
        1. Uses ESearch to retrieve PMC IDs in paginated batches.
        2. Uses EFetch to download each article XML individually.
        3. Saves each XML response as PMC_<pmcid>.xml.
        4. Sleeps between requests to respect NCBI rate limits.

    Args:
        query (str): PMC search query string using Entrez/MeSH syntax.
        total_articles (int): Total number of matching articles expected.
        batch_size (int): Number of PMC IDs retrieved per ESearch batch.
        output_dir_path (str): Directory where XML files will be saved.
        client_email (str, optional): Contact email sent to NCBI. Defaults to "".
        client_tool (str, optional): Tool name sent to NCBI. Defaults to "".
        initial_count (int, optional): Starting offset for resumed downloads. Defaults to 0.

    Returns:
        None.
    """
    # Ensure the output directory exists before writing files.
    os.makedirs(output_dir_path, exist_ok=True)

    # Initialize NCBI E-utilities clients.
    esearch_client: ESearchClient = ESearchClient(tool=client_tool, email=client_email)
    efetch_client: EFetchClient = EFetchClient(tool=client_tool, email=client_email)

    # Track the total progress across all expected articles.
    with tqdm(total=total_articles, desc="Processing Articles", initial=initial_count) as pbar:
        # Iterate over the full result set using ESearch pagination.
        for start in range(initial_count, total_articles, batch_size):
            # Parameters for retrieving a batch of PMC IDs.
            esearch_parameters: Dict[str, Any] = {
                "db": "pmc",
                "term": query,
                "retstart": start,
                "retmax": batch_size,
                "usehistory": False,
            }

            # Retrieve PMC IDs for the current batch.
            esearch_result: Dict[str, Any] = esearch_client.run_query(**esearch_parameters)
            ids: List[str] = esearch_result["ids"]

            # Download each article XML individually.
            for pmcid in ids:
                # Parameters for downloading one PMC article.
                efetch_parameters: Dict[str, Any] = {
                    "db": "pmc",
                    "id": [pmcid],
                    "query_key": None,
                    "webenv": None,
                }

                # Fetch raw XML from PMC.
                efetch_result: str = efetch_client.fetch_records(**efetch_parameters)

                # Save XML content using the PMC_<pmcid>.xml convention.
                xml_filename: str = os.path.join(output_dir_path, f"PMC_{pmcid}.xml")
                with open(xml_filename, "w", encoding="utf-8") as f:
                    f.write(efetch_result)

                # Respect NCBI usage guidelines by spacing requests.
                time.sleep(1)

                # Update progress after each successfully saved XML file.
                pbar.update(1)


def configure_logging(level: str) -> None:
    """
    Configure the root logger.

    Args:
        level (str): Logging level name, such as "DEBUG", "INFO", "WARNING", or "ERROR".

    Returns:
        None.
    """
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=getattr(logging, level.upper(), logging.INFO),
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def get_total_articles(esearch: ESearchClient, query: str) -> int:
    """
    Retrieve the total number of PMC articles matching a query.

    Args:
        esearch (ESearchClient): Initialized ESearch client.
        query (str): PMC search query string.

    Returns:
        int: Total number of matching articles.
    """
    # retmax=0 asks ESearch for the count without returning article IDs.
    result: Dict[str, Any] = esearch.run_query(
        db="pmc",
        term=query,
        retmax=0,
        usehistory=False,
    )

    return int(result["count"])


def ensure_directory(path: Path) -> None:
    """
    Ensure that an output directory exists.

    Args:
        path (Path): Directory path to create if missing.

    Returns:
        None.
    """
    path.mkdir(parents=True, exist_ok=True)


def download_batches(
    query: str,
    total: int,
    batch_size: int,
    output_dir: Path,
    email: str,
    tool: str,
    initial_offset: int = 0,
) -> None:
    """
    Download PMC articles in batches, resuming from an existing file count.

    Args:
        query (str): PMC search query string.
        total (int): Total number of matching articles.
        batch_size (int): Number of PMC IDs requested per batch.
        output_dir (Path): Directory where XML files are saved.
        email (str): Contact email sent to NCBI.
        tool (str): Tool name sent to NCBI.
        initial_offset (int, optional): Starting offset for resumed downloads. Defaults to 0.

    Returns:
        None.
    """
    # Start from the number of files already present in the output directory.
    offset: int = initial_offset

    # Keep downloading until the expected total count is reached.
    while offset < total:
        batch_process(
            query=query,
            total_articles=total,
            batch_size=batch_size,
            output_dir_path=str(output_dir),
            client_email=email,
            client_tool=tool,
            initial_count=offset,
        )

        # Recompute progress from files currently stored on disk.
        offset = len(list(output_dir.iterdir()))
        logging.debug("Downloaded %d/%d files so far", offset, total)


def main() -> None:
    """
    Command-line entry point for downloading PMC XML articles.

    This function:
        1. Parses CLI arguments.
        2. Configures logging.
        3. Creates the output directory.
        4. Counts matching PMC articles with ESearch.
        5. Downloads XML files with EFetch.
        6. Retries after temporary HTTP/network errors.

    Returns:
        None.
    """
    # Define CLI arguments.
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Batch-download PMC articles matching a MeSH query."
    )

    parser.add_argument("--query", required=True, help="PMC search query (MeSH terms + filters)")

    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory to save PMC XML articles",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Number of articles to fetch per batch",
    )

    parser.add_argument(
        "--email",
        required=True,
        help="NCBI-registered email address",
    )

    parser.add_argument(
        "--tool",
        default="pmc_tool",
        help="NCBI tool name for API usage tracking",
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )

    # Parse command-line arguments.
    args: argparse.Namespace = parser.parse_args()

    # Configure runtime logging and ensure output path exists.
    configure_logging(args.log_level)
    ensure_directory(args.output_dir)

    logging.info("Starting PMC download with query:\n%s", args.query)

    # Initialize ESearch client only for counting matching records.
    esearch_client: ESearchClient = ESearchClient(tool=args.tool, email=args.email)

    # Count how many articles match the query before downloading.
    total_articles: int = get_total_articles(esearch_client, args.query)
    logging.info("Total matching articles: %d", total_articles)

    try:
        # Start or resume batch downloading.
        download_batches(
            query=args.query,
            total=total_articles,
            batch_size=args.batch_size,
            output_dir=args.output_dir,
            email=args.email,
            tool=args.tool,
            initial_offset=len(list(args.output_dir.iterdir())),
        )

    except (HTTPError, ChunkedEncodingError, ConnectionError) as e:
        # Handle temporary network/API errors by pausing and retrying.
        logging.error("HTTP error during download: %s", e)
        logging.info("Pausing for 60 seconds before retrying")
        time.sleep(60)

        # Retry the CLI workflow.
        main()

    logging.info("Download complete: %d articles saved to %s", total_articles, args.output_dir)


if __name__ == "__main__":
    main()