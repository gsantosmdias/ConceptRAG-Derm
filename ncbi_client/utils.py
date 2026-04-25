import xml.etree.ElementTree as ET
from typing import List, Dict, Any, Set, Optional
from pubmed_parser.utils import read_xml as read_pubmed_xml, stringify_children
from pubmed_parser.pubmed_oa_parser import parse_article_meta, parse_pubmed_references
import spacy
import re


def build_reference_lookup_tables(reference_list: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Build a lookup table mapping numeric reference identifiers to reference metadata.

    This function extracts numeric identifiers from `ref_id` fields (e.g., "bib005" → "5")
    and constructs a dictionary for fast lookup.

    Args:
        reference_list (List[Dict[str, Any]]): List of reference dictionaries from PubMed parser.

    Returns:
        Dict[str, Dict[str, Any]]:
            Mapping from normalized reference number (as string) → reference metadata dict.
            Fields 'pmid' and 'pmc' are removed from each reference.
    """
    ref_number_lookup: Dict[str, Dict[str, Any]] = {}

    for ref in reference_list:
        ref_id: str = ref["ref_id"]

        # Normalize reference ID (e.g., "bib005" → "5")
        if "bib" in ref_id:
            ref_id = ref_id.split("bib")[1]

        num_match = re.findall(r"(\d+)", ref_id, re.IGNORECASE)
        if num_match:
            num = num_match[0]
            num = num.lstrip("0") or "0"
            ref_number_lookup[num] = ref

    # Remove unnecessary keys
    for key in ref_number_lookup:
        ref_number_lookup[key] = {
            k: v for k, v in ref_number_lookup[key].items()
            if k not in ['pmid', 'pmc']
        }

    return ref_number_lookup


def parse_pubmed_paragraph_refactor(path: str) -> List[Dict[str, Any]]:
    """
    Extract structured paragraph data from a PubMed OA XML file.

    This function:
        - Extracts article metadata (DOI, title, PMID, PMC)
        - Retrieves all <p> elements from <body>
        - Excludes supplementary sections
        - Captures section titles and reference IDs per paragraph

    Args:
        path (str): Path to PubMed OA XML file.

    Returns:
        List[Dict[str, Any]]:
            List of paragraph dictionaries containing:
                - article metadata
                - section name
                - text content
                - reference IDs
    """
    tree = read_pubmed_xml(path)
    dict_article_meta = parse_article_meta(tree)

    pmid: str = dict_article_meta["pmid"]
    pmc: str = dict_article_meta["pmc"]

    # DOI extraction
    doi_elem = tree.xpath("//article-id[@pub-id-type='doi']")
    article_doi: str = doi_elem[0].text.strip() if doi_elem else ""

    # Title extraction
    title_elem = tree.xpath("//article-title")
    article_title: str = stringify_children(title_elem[0]).strip() if title_elem else ""

    paragraphs: List[Dict[str, Any]] = []

    for paragraph in tree.xpath('//body//p'):
        skip: bool = False

        # Skip supplementary sections
        for ancestor in paragraph.iterancestors():
            if ancestor.tag == "sec" and ancestor.get("sec-type") == "supplementary-material":
                skip = True
                break
            if ancestor.tag == "body":
                break

        if skip:
            continue

        # Extract section title
        section: str = ""
        for anc in paragraph.iterancestors():
            title = anc.find("title")
            if title is not None:
                section = stringify_children(title).strip()
                break

        # Extract reference IDs
        ref_ids: List[str] = []
        for child in paragraph.getchildren():
            if "rid" in child.attrib:
                ref_ids.append(child.attrib["rid"])

        paragraphs.append({
            "article_doi": article_doi,
            "article_title": article_title,
            "pmc": pmc,
            "pmid": pmid,
            "reference_ids": ref_ids,
            "section": section,
            "text": stringify_children(paragraph)
        })

    return paragraphs


def merge_paragraphs_with_references_json(
    efetch_response: str
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Merge paragraph text with full reference metadata.

    Args:
        efetch_response (str): XML string from EFetch.

    Returns:
        Dict[str, List[Dict[str, Any]]]:
            {"data": [...]} where each entry contains enriched paragraph data.
    """
    paragraphs = parse_pubmed_paragraph_refactor(efetch_response)
    references = parse_pubmed_references(efetch_response)

    if not references:
        return paragraphs

    # Build reference index
    ref_index: Dict[tuple, Dict[str, Any]] = {
        (ref["pmid"], ref["ref_id"]): ref for ref in references
    }

    merged_list: List[Dict[str, Any]] = []

    for para in paragraphs:
        pmid: str = para["pmid"]
        ref_ids: List[str] = para.get("reference_ids", [])

        # Deduplicate while preserving order
        seen: Set[str] = set()
        dedup_ref_ids = [rid for rid in ref_ids if not (rid in seen or seen.add(rid))]

        # Expand references
        enriched_refs = [
            ref_index.get((pmid, rid), {"ref_id": rid, "missing": True})
            for rid in dedup_ref_ids
        ]

        merged_list.append({
            "text": para["text"],
            "section": para["section"],
            "article_doi": para["article_doi"],
            "article_title": para["article_title"],
            "reference_ids": dedup_ref_ids,
            "references": enriched_refs
        })

    return {"data": merged_list}


def extract_ids_from_esummary(xml_str: str) -> List[str]:
    """
    Extract document IDs from an ESummary XML response.

    Args:
        xml_str (str): XML response string.

    Returns:
        List[str]: List of document IDs.
    """
    root = ET.fromstring(xml_str)
    return [doc.find("Id").text for doc in root.findall(".//DocSum")]

def spacy_extract_abbreviations(model_name: str, text: str) -> Dict[str, str]:
    """
    Extract abbreviation → expansion mappings using spaCy + SciSpacy.

    This function:
        1. Loads the specified spaCy model.
        2. Ensures the abbreviation detector is present.
        3. Processes the input text.
        4. Extracts unique abbreviation → long-form mappings.

    Args:
        model_name (str): Name of the spaCy model (e.g., "en_core_web_sm").
        text (str): Input text.

    Returns:
        Dict[str, str]: Mapping of abbreviation → expansion.
    """
    # Load spaCy model
    nlp = spacy.load(model_name)

    # Ensure abbreviation detector is available
    if "abbreviation_detector" not in nlp.pipe_names:
        nlp.add_pipe("abbreviation_detector", last=True)

    # Process text
    doc = nlp(text)

    # Build abbreviation mapping
    abbr_map: Dict[str, str] = {}
    for abbr in doc._.abbreviations:
        short: str = abbr.text
        longf: str = abbr._.long_form.text

        # Keep first occurrence only
        if short not in abbr_map:
            abbr_map[short] = longf

    return abbr_map


def regex_extract_abbreviations(text: str) -> Dict[str, str]:
    """
    Extract abbreviation → expansion pairs using regex heuristics.

    Strategy:
        - Identify patterns like "(ABC)" where ABC is uppercase.
        - Extract the preceding N words as the expansion (N = len(ABC)).

    Args:
        text (str): Input text.

    Returns:
        Dict[str, str]: Mapping of abbreviation → expansion.
    """
    results: Dict[str, str] = {}

    # Pattern for parentheses without spaces
    paren_re = re.compile(r'\(([^\s)]+)\)')

    # Word tokenizer (supports hyphenated tokens)
    word_re = re.compile(r'\b[\w-]+\b')

    for m in paren_re.finditer(text):
        abbr: str = m.group(1)

        # Ensure abbreviation is uppercase (letters only)
        letters: List[str] = [c for c in abbr if c.isalpha()]
        if not letters or not all(c.isupper() for c in letters):
            continue

        n: int = len(abbr)
        before: str = text[:m.start()]

        words = list(word_re.finditer(before))
        if not words:
            continue

        # Select last N tokens as expansion
        selected = words[-n:]
        start_idx: int = selected[0].start()
        end_idx: int = selected[-1].end()

        expansion: str = before[start_idx:end_idx]
        results[abbr] = expansion

    return results


def expand_abbreviations_in_xml(
    xml_str: str,
    spacy_model: str = "en_core_web_sm"
) -> Dict[str, Any]:
    """
    Expand abbreviations inside structured XML-derived paragraph data.

    Workflow:
        1. Merge paragraphs and references.
        2. Extract global abbreviation mapping via spaCy.
        3. For each paragraph:
            - Replace abbreviations using spaCy mapping
            - Remove parenthetical expansions
            - Apply regex-based extraction for remaining cases
            - Replace and clean again

    Args:
        xml_str (str): Raw XML string.
        spacy_model (str): spaCy model name.

    Returns:
        Dict[str, Any]:
            Same structure as merge_paragraphs_with_references_json(),
            but with expanded abbreviations.
    """
    # Step 1: merge paragraphs
    paragraphs: Dict[str, Any] = merge_paragraphs_with_references_json(xml_str)

    # Step 2: global abbreviation map
    spacy_map: Dict[str, str] = spacy_extract_abbreviations(spacy_model, xml_str)

    # Step 3: process each paragraph
    for item in paragraphs["data"]:
        text: str = item["text"]

        # spaCy-based replacements
        for abbr, full in spacy_map.items():
            text = text.replace(abbr, full)
            text = text.replace(f"({full})", "")

        # Regex-based fallback
        regex_map: Dict[str, str] = regex_extract_abbreviations(text)

        for abbr, full in regex_map.items():
            text = text.replace(abbr, full)
            text = text.replace(f"({full})", "")

        item["text"] = text

    return paragraphs


def extract_refs_from_pmc_text(
    chunk_text: str,
    ref_number_lookup: Dict[int, Dict[str, Any]]
) -> Dict[int, Dict[str, Any]]:
    """
    Extract numeric references from PMC text and resolve them to metadata.

    Supports:
        - Single refs: [5]
        - Ranges: [3-7]
        - Lists: [2,5,7]
        - Mixed: [1-3,7]

    Args:
        chunk_text (str): Input text.
        ref_number_lookup (Dict[int, Dict[str, Any]]): Lookup table for references.

    Returns:
        Dict[int, Dict[str, Any]]:
            Mapping of reference number → reference metadata.
    """
    pattern = re.compile(r"\[([^\]]+)\]")

    refs_found_numbers: Set[int] = set()

    for match in pattern.findall(chunk_text):
        parts = re.split(r",\s*", match)

        for part in parts:
            part = part.strip()

            # Range case
            range_match = re.match(r"(\d+)\s*[\u2013\-]\s*(\d+)", part)
            if range_match:
                start: int = int(range_match.group(1))
                end: int = int(range_match.group(2))

                for n in range(start, end + 1):
                    if n in ref_number_lookup:
                        refs_found_numbers.add(n)
                continue

            # Single number case
            if part.isdigit():
                n = int(part)
                if n in ref_number_lookup:
                    refs_found_numbers.add(n)

    full_refs: Dict[int, Dict[str, Any]] = {}

    for n in sorted(refs_found_numbers):
        ref = ref_number_lookup[n]
        full_refs[n] = {k: v for k, v in ref.items() if k not in ['ref_id']}

    return full_refs


def _parse_date(tree: Any, date_type: str) -> Dict[str, str]:
    """
    Extract publication date components from XML.

    Args:
        tree: XML tree.
        date_type (str): Date type (e.g., 'ppub', 'collection').

    Returns:
        Dict[str, str]: Dictionary with keys 'year', 'month', 'day'.
    """
    def get_text(node):
        return node.text if node is not None else None

    pub_date_path = f".//pub-date[@pub-type='{date_type}' or @date-type='{date_type}']"
    date_node = tree.xpath(pub_date_path)

    if not date_node:
        return {}

    date_dict: Dict[str, str] = {}

    for part in ["year", "month", "day"]:
        text = get_text(date_node[0].find(part))
        if text is not None:
            date_dict[part] = text

    return date_dict


def _format_date(date_dict: Dict[str, str]) -> str:
    """
    Format date dictionary into string.

    Args:
        date_dict (Dict[str, str]): Date components.

    Returns:
        str: Formatted date string.
    """
    day: str = date_dict.get("day", "01")
    month: str = date_dict.get("month", "01")
    year: str = date_dict.get("year", "")

    return f"{day}-{month}-{year}" if year else f"{day}-{month}"


def get_author_list(xml_tree: Any) -> List[List[str]]:
    """
    Extract author names from XML.

    Args:
        xml_tree: XML tree.

    Returns:
        List[List[str]]: List of [surname, given-names].
    """
    tree_author = xml_tree.xpath('.//contrib-group/contrib[@contrib-type="author"]')
    author_list: List[List[str]] = []

    for author in tree_author:
        try:
            author_list.append([
                author.find("name/surname").text,
                author.find("name/given-names").text
            ])
        except BaseException:
            author_list.append(["", ""])

    return author_list

def parse_pubmed_xml(path: str) -> Dict[str, Any]:
    """
    Parse a PubMed Open Access (PMC) XML file into a structured dictionary.

    This function extracts:
        - Article metadata (PMID, DOI, title, journal, publication year)
        - Author list
        - Full article text (merged paragraphs)
        - Reference metadata

    Notes:
        - Paragraphs inside <sec sec-type="supplementary-material"> are excluded.
        - Text is flattened into a single string (joined by newline).
        - References are optionally converted into a lookup table.

    Args:
        path (str): Path to the PubMed OA XML file.

    Returns:
        Dict[str, Any]:
            Dictionary containing:
                - pmid (str)
                - article_doi (str)
                - article_title (str)
                - publication_year (Optional[int])
                - author_list (List[List[str]])
                - journal (str)
                - text (Optional[str])
                - references (Optional[Dict])
    """
    # Load XML tree
    tree = read_pubmed_xml(path)

    # Extract authors
    author_list: List[List[str]] = get_author_list(tree)

    # Extract journal name
    journal_node = tree.findall(".//journal-title")
    if journal_node is not None:
        journal: str = " ".join(["".join(node.itertext()) for node in journal_node])
    else:
        journal = ""

    # Extract publication date
    pub_date_dict: Dict[str, str] = _parse_date(tree, "ppub")
    if "year" not in pub_date_dict:
        pub_date_dict = _parse_date(tree, "collection")

    pub_date: str = _format_date(pub_date_dict)

    # Extract publication year safely
    try:
        pub_year: Optional[int] = int(pub_date_dict.get("year"))
    except TypeError:
        pub_year = None

    # Extract references
    references = parse_pubmed_references(path)
    if references:
        references = build_reference_lookup_tables(references)

    # Extract metadata
    dict_article_meta: Dict[str, Any] = parse_article_meta(tree)
    pmid: str = dict_article_meta["pmid"]

    # -------------------------------
    # Extract DOI
    # -------------------------------
    doi_elem = tree.xpath("//article-id[@pub-id-type='doi']")
    article_doi: str = doi_elem[0].text.strip() if doi_elem else ""

    # -------------------------------
    # Extract Title
    # -------------------------------
    title_elem = tree.xpath("//article-title")
    article_title: str = ""
    if title_elem:
        article_title = stringify_children(title_elem[0]).strip()

    # -------------------------------
    # Extract Paragraph Text
    # -------------------------------
    paragraphs: List[str] = []

    for paragraph in tree.xpath('//body//p'):
        skip: bool = False

        # Skip supplementary sections
        for ancestor in paragraph.iterancestors():
            if ancestor.tag == "sec" and ancestor.get("sec-type") == "supplementary-material":
                skip = True
                break
            if ancestor.tag == "body":
                break

        if skip:
            continue

        paragraphs.append(stringify_children(paragraph))

    # Merge all paragraphs into a single text block
    text: Optional[str] = "\n".join(paragraphs)
    if text == "":
        text = None

    # Final structured output
    data: Dict[str, Any] = {
        "pmid": pmid,
        "article_doi": article_doi,
        "article_title": article_title,
        "publication_year": pub_year,
        "author_list": author_list,
        "journal": journal,
        "text": text,
        "references": references
    }

    return data


def expand_abbreviations(
    text: str,
    spacy_model: str = "en_core_web_sm"
) -> str:
    """
    Expand abbreviations in a text using spaCy and regex fallback.

    Workflow:
        1. Attempt spaCy-based abbreviation detection.
        2. Replace abbreviations with their long forms.
        3. Remove parenthetical long forms "(Long Form)".
        4. Apply regex-based extraction for missed abbreviations.
        5. Perform replacement and cleanup again.

    Notes:
        - If spaCy fails due to text length ([E088]), fallback to regex only.
        - Uses simple string replacement (no token-level alignment).

    Args:
        text (str): Input text.
        spacy_model (str, optional): spaCy model name. Defaults to "en_core_web_sm".

    Returns:
        str: Text with abbreviations expanded and cleaned.
    """
    # Step 1: spaCy-based extraction
    try:
        spacy_map: Optional[Dict[str, str]] = spacy_extract_abbreviations(spacy_model, text)
    except ValueError as e:
        if "[E088]" in str(e):
            print("⚠ Text too long for spaCy. Falling back to regex only...")
            spacy_map = None
        else:
            raise

    # Step 2–3: Replace spaCy-detected abbreviations
    if spacy_map:
        for abbr, full in spacy_map.items():
            text = text.replace(abbr, full)
            text = text.replace(f"({full})", "")

    # Step 4: regex-based extraction
    regex_map: Dict[str, str] = regex_extract_abbreviations(text)

    # Step 5: Replace regex-detected abbreviations
    for abbr, full in regex_map.items():
        text = text.replace(abbr, full)
        text = text.replace(f"({full})", "")

    return text