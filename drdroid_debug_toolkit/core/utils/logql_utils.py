"""
LogQL Query Utilities

This module provides functions for cleaning, validating, and normalizing LogQL queries
before execution against Loki. These utilities help prevent common 400 errors caused by:
- Double-encoded quotes
- Unicode/smart quotes
- Empty selectors
- Whitespace/newline issues
- Invalid regex patterns
"""
import re
import logging

logger = logging.getLogger(__name__)


class LogQLValidationError(Exception):
    """Raised when a LogQL query fails validation."""
    pass


def cleanup_logql_query(query: str, validate: bool = True) -> str:
    """
    Clean up and optionally validate a LogQL query.

    Performs the following transformations:
    1. Strip leading/trailing whitespace
    2. Fix double-encoded quotes (\\" -> ")
    3. Normalize unicode/smart quotes to standard double quotes
    4. Collapse multiple whitespace/newlines into single spaces (preserving quoted content)
    5. Optionally validate the query structure

    Args:
        query: The LogQL query string to clean up
        validate: If True, validates the query and raises LogQLValidationError for invalid queries

    Returns:
        The cleaned query string

    Raises:
        LogQLValidationError: If validate=True and the query is invalid
    """
    if not query:
        if validate:
            raise LogQLValidationError("Empty query provided")
        return query

    original_query = query

    # Step 1: Strip leading/trailing whitespace
    query = query.strip()

    # Step 2: Fix double-encoded quotes (happens when JSON is serialized twice)
    # Pattern: \" should become " (but only outside of already valid quoted strings)
    query = _fix_double_encoded_quotes(query)

    # Step 3: Normalize unicode/smart quotes to standard double quotes
    query = _normalize_quotes(query)

    # Step 4: Collapse whitespace while preserving quoted content
    query = _collapse_whitespace(query)

    # Log if query was modified
    if query != original_query:
        logger.debug(f"LogQL query cleaned: {repr(original_query)} -> {repr(query)}")

    # Step 5: Validate if requested
    if validate:
        _validate_logql_query(query)

    return query


def _fix_double_encoded_quotes(query: str) -> str:
    """
    Fix double-encoded quotes in a LogQL query.

    When a query passes through multiple JSON serialization layers,
    quotes can become double-encoded: " -> \" -> \\\"

    This function detects and fixes these patterns.
    """
    # Check for double-encoded pattern: \" appearing in selector position
    # Pattern: {label=\\"value\\"} should become {label="value"}

    # First, handle \\\" (triple-escaped) -> \"
    if '\\\\\\"' in query or "\\\\\"" in query:
        query = query.replace('\\\\\\"', '\\"')
        query = query.replace("\\\\\"", '\\"')

    # Then handle \\" (double-escaped) -> "
    # Be careful: we want to fix {label=\"value\"} but not break valid escapes inside strings

    # Check if the query has the double-encoding pattern in label selectors
    # Pattern: {label=\\"value\\"} or {label=\\\"value\\\"}
    double_encoded_pattern = r'\{([^}]*?)=\\"([^"]*?)\\"\}'
    if re.search(double_encoded_pattern, query):
        # Fix the pattern
        query = re.sub(double_encoded_pattern, r'{\1="\2"}', query)

    # Also check for standalone backslash-quote outside of valid strings
    # This happens when JSON.stringify is called on already-stringified content
    # Pattern: looking for \" that's not inside a properly quoted string

    # Simple heuristic: if we see \" followed by content and another \"
    # in a context that looks like a label selector, fix it
    standalone_pattern = r'=\\"([^\\]*?)\\"'
    if re.search(standalone_pattern, query):
        query = re.sub(standalone_pattern, r'="\1"', query)

    return query


def _normalize_quotes(query: str) -> str:
    """
    Normalize various quote styles to standard double quotes.

    Handles:
    - Unicode left/right double quotes: " " -> "
    - Unicode left/right single quotes: ' ' -> ' (in non-LogQL contexts)
    - Backticks in label values: ` -> " (only in selector context, not regex)
    """
    # Replace unicode double quotes with standard double quotes
    # Left double quotation mark: U+201C
    # Right double quotation mark: U+201D
    query = query.replace('\u201c', '"')  # "
    query = query.replace('\u201d', '"')  # "

    # Replace unicode single quotes (these would cause errors in LogQL)
    # Left single quotation mark: U+2018
    # Right single quotation mark: U+2019
    query = query.replace('\u2018', "'")  # '
    query = query.replace('\u2019', "'")  # '

    # Note: We don't convert single quotes to double quotes because
    # single quotes are invalid in LogQL and we want validation to catch this

    # Note: We don't convert backticks in line filters because they're valid there
    # Only fix backticks in label selectors: {label=`value`} -> {label="value"}
    backtick_selector_pattern = r'\{([^}]*?)=`([^`]*?)`'
    if re.search(backtick_selector_pattern, query):
        query = re.sub(backtick_selector_pattern, r'{\1="\2"', query)

    return query


def _collapse_whitespace(query: str) -> str:
    """
    Collapse multiple whitespace/newlines into single spaces,
    while preserving content inside double quotes and backticks.
    """
    if not query:
        return query

    # Split by quoted strings (double quotes and backticks) to preserve them
    # Pattern matches: "content" or `content`
    # Also handles escaped quotes inside strings
    parts = re.split(r'("(?:[^"\\]|\\.)*"|`[^`]*`)', query)
    cleaned_parts = []

    for i, part in enumerate(parts):
        if i % 2 == 0:  # Non-quoted part
            # Collapse multiple whitespace/newlines into single space
            part = re.sub(r'\s+', ' ', part)
        cleaned_parts.append(part)

    return ''.join(cleaned_parts)


def _validate_logql_query(query: str) -> None:
    """
    Validate a LogQL query and raise LogQLValidationError if invalid.

    Checks for:
    - Empty selector {}
    - Empty-compatible matchers like {job=~".*"}
    - Single quotes in label values
    - Unbalanced parentheses in regex
    - Unbalanced brackets in regex
    """
    # Check for empty selector
    if re.match(r'^\s*\{\s*\}\s*', query):
        raise LogQLValidationError(
            "Empty selector {} is not allowed. "
            "Queries require at least one label matcher, e.g., {job=~\".+\"}"
        )

    # Check for empty-compatible matchers: {label=~".*"} or {label=""} or {label=~""}
    # These effectively match everything and are rejected by Loki
    empty_matcher_patterns = [
        r'\{[^}]*=~"\.\*"[^}]*\}',  # {label=~".*"}
        r'\{[^}]*=""\s*\}',         # {label=""}
        r'\{[^}]*=~""\s*\}',        # {label=~""}
    ]

    for pattern in empty_matcher_patterns:
        if re.search(pattern, query):
            # Check if there's at least one non-empty matcher
            # Extract all matchers and check if any is non-empty
            matchers = re.findall(r'(\w+)\s*([=!~]+)\s*"([^"]*)"', query)
            has_valid_matcher = False
            for label, op, value in matchers:
                if op in ('=', '=~') and value and value != '.*':
                    has_valid_matcher = True
                    break
                if op in ('!=', '!~'):
                    has_valid_matcher = True
                    break

            if not has_valid_matcher:
                raise LogQLValidationError(
                    f"Query has only empty-compatible matchers. "
                    f"Use a non-empty pattern like {{job=~\".+\"}} instead."
                )

    # Check for single quotes in label selectors (invalid in LogQL)
    single_quote_selector = r"\{[^}]*=\s*'[^']*'"
    if re.search(single_quote_selector, query):
        raise LogQLValidationError(
            "Single quotes are not valid in LogQL label selectors. Use double quotes instead."
        )

    # Check for obviously broken regex in line filters
    line_filter_regex = re.findall(r'\|~\s*"([^"]*)"', query)
    for regex_pattern in line_filter_regex:
        # Check for unbalanced parentheses
        if regex_pattern.count('(') != regex_pattern.count(')'):
            raise LogQLValidationError(
                f"Unbalanced parentheses in regex pattern: {regex_pattern}"
            )

        # Check for unbalanced brackets
        if regex_pattern.count('[') != regex_pattern.count(']'):
            raise LogQLValidationError(
                f"Unbalanced brackets in regex pattern: {regex_pattern}"
            )


def is_valid_logql_query(query: str) -> tuple[bool, str]:
    """
    Check if a LogQL query is valid.

    Args:
        query: The LogQL query to validate

    Returns:
        Tuple of (is_valid, error_message)
        If valid, error_message is empty string.
    """
    try:
        cleanup_logql_query(query, validate=True)
        return True, ""
    except LogQLValidationError as e:
        return False, str(e)
    except Exception as e:
        return False, f"Unexpected error: {e}"
