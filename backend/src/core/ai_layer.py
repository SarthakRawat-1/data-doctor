"""Bounded AI layer for formatting outputs.

CRITICAL CONSTRAINT: AI is ONLY used for formatting, NOT decision-making.

This module takes the deterministic JSON output from the core engines
and formats it into:
- SQL scripts
- Markdown documentation
- Slack messages

Phase 5 Implementation (Optional for MVP).
"""
from src.schemas import DiagnosisResponse, SuggestedFix


def generate_sql_script(fix: SuggestedFix) -> str:
    """
    Generate SQL script for a fix suggestion.
    
    Uses LLM to convert structured fix object into executable SQL.
    
    Args:
        fix: Structured fix suggestion
    
    Returns:
        SQL script as string
    """
    # TODO: Phase 5 - Implement
    # Use LLM to generate SQL based on fix.action and fix.target
    # Example: For UPDATE_SCHEMA, generate ALTER TABLE statements
    raise NotImplementedError("Phase 5 - Optional")


def generate_markdown_report(diagnosis: DiagnosisResponse) -> str:
    """
    Generate formatted Markdown report.
    
    Args:
        diagnosis: Complete diagnosis response
    
    Returns:
        Markdown-formatted report
    """
    # TODO: Phase 5 - Implement
    # Use LLM to format the JSON into readable Markdown
    raise NotImplementedError("Phase 5 - Optional")


def generate_slack_message(diagnosis: DiagnosisResponse) -> dict:
    """
    Generate Slack Block Kit message.
    
    Args:
        diagnosis: Complete diagnosis response
    
    Returns:
        Slack Block Kit JSON
    """
    # TODO: Phase 5 - Implement
    # Use LLM to format into Slack blocks
    raise NotImplementedError("Phase 5 - Optional")
