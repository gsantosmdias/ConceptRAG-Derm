import os
import pandas as pd
from tqdm import tqdm
from typing import List, Dict, Any
from ncbi_client.utils import parse_pubmed_xml
from ncbi_client.utils import expand_abbreviations
import json


def build_silver_layer(
    dir_path: str,
    output_path: str,
    batch_size: int = 10000,
    spacy_model: str = "en_core_web_sm"
) -> None:
    """
    Process raw PMC XML files and save parsed articles as Parquet batches.

    This function reads XML files from a directory, parses each article,
    expands abbreviations in the article text, and writes valid articles
    to disk as Parquet files in batches.

    Args:
        dir_path (str): Directory containing raw PMC XML files.
        output_path (str): Directory where Parquet batch files will be saved.
        batch_size (int, optional): Number of valid articles per Parquet batch.
            Defaults to 10000.
        spacy_model (str, optional): spaCy model used for abbreviation expansion.
            Defaults to "en_core_web_sm".

    Returns:
        None.
    """

    # Collect all file paths from the input directory.
    files: List[str] = [
        os.path.join(dir_path, f)
        for f in os.listdir(dir_path)
        if os.path.isfile(os.path.join(dir_path, f))
    ]

    # Ensure the output directory exists.
    os.makedirs(output_path, exist_ok=True)

    # Temporary in-memory buffer for valid parsed articles.
    valid_articles: List[Dict[str, Any]] = []

    # Counters for monitoring progress.
    total_processed: int = 0
    batches_written: int = 0

    # Iterate over all raw XML files.
    for file in tqdm(files, desc="Processing PMC XML files"):
        try:
            # Parse XML into a structured dictionary.
            data: Dict[str, Any] = parse_pubmed_xml(file)

            # Keep only articles with extracted text.
            if data.get("text"):
                # Expand abbreviations before saving the article text.
                data["text"] = expand_abbreviations(
                    data["text"],
                    spacy_model=spacy_model
                )

                # Add parsed article to the current batch buffer.
                valid_articles.append(data)

                # Write batch once the buffer reaches batch_size.
                if len(valid_articles) >= batch_size:
                    _write_batch_to_parquet(valid_articles, output_path, batches_written)

                    batches_written += 1
                    total_processed += len(valid_articles)

                    # Reset buffer after writing the batch.
                    valid_articles = []

        except Exception as e:
            # Keep processing even if one XML file fails.
            print(f"⚠️ Error processing {file}: {e}")

    # Write remaining articles that did not fill a complete batch.
    if valid_articles:
        _write_batch_to_parquet(valid_articles, output_path, batches_written)
        total_processed += len(valid_articles)

    print(f"✅ Finished processing {total_processed} valid articles.")


def _write_batch_to_parquet(
    articles: List[Dict[str, Any]],
    output_path: str,
    batch_index: int
) -> None:
    """
    Write a batch of parsed articles to a Parquet file.

    Args:
        articles (List[Dict[str, Any]]): Parsed article dictionaries.
        output_path (str): Directory where the Parquet file will be saved.
        batch_index (int): Batch index used in the output filename.

    Returns:
        None.
    """

    # Convert list of article dictionaries into a tabular DataFrame.
    df: pd.DataFrame = pd.DataFrame(articles)

    # Serialize nested reference dictionaries/lists as JSON strings.
    if "references" in df.columns:
        df["references"] = df["references"].apply(
            lambda x: json.dumps(x)
        )

    # Build batch filename using output directory basename and batch index.
    filename: str = f"{os.path.basename(output_path)}_batch_{batch_index}.parquet"
    batch_file: str = os.path.join(output_path, filename)

    # Save the batch as a Parquet file.
    df.to_parquet(batch_file, index=False)

    print(f"✅ Wrote batch {batch_index} to {batch_file}")


if __name__ == "__main__":
    import argparse

    # Define command-line interface for the XML processing pipeline.
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Build the Silver Layer from PMC XML articles."
    )

    parser.add_argument(
        "--dir_path",
        type=str,
        required=True,
        help="Path to the Bronze folder containing raw PMC XML files."
    )

    parser.add_argument(
        "--output_path",
        type=str,
        required=True,
        help=(
            "Base path for output Parquet files. Each batch will be saved "
            "with an incremented batch index suffix."
        )
    )

    parser.add_argument(
        "--batch_size",
        type=int,
        default=10000,
        help="Number of articles to process per batch. Default is 10,000."
    )

    parser.add_argument(
        "--spacy_model",
        type=str,
        default="en_core_web_sm",
        help="spaCy model to use for abbreviation expansion."
    )

    # Parse CLI arguments.
    args: argparse.Namespace = parser.parse_args()

    # Run the processing pipeline.
    build_silver_layer(
        dir_path=args.dir_path,
        output_path=args.output_path,
        batch_size=args.batch_size,
        spacy_model=args.spacy_model
    )