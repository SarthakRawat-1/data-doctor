"""Bounded AI layer for formatting outputs.

CRITICAL CONSTRAINT: AI is ONLY used for formatting, NOT decision-making.

This module takes the deterministic JSON output from the core engines
and formats it into:
- SQL scripts
- Markdown documentation
- Slack messages

Phase 5 Implementation using Groq API.

References:
- Groq API: https://console.groq.com/docs/structured-outputs
- Slack Webhooks: https://api.slack.com/messaging/webhooks
"""
from groq import Groq
from slack_sdk.webhook import WebhookClient

from src.config import settings
from src.constants import FixAction, Severity
from src.schemas import DiagnosisResponse, SuggestedFix


def _get_groq_client() -> Groq:
    """
    Get Groq client instance.
    
    Returns:
        Groq client
    
    Raises:
        ValueError: If GROQ_API_KEY is not configured
    """
    if not settings.GROQ_API_KEY:
        raise ValueError("GROQ_API_KEY not configured in environment")
    
    return Groq(api_key=settings.GROQ_API_KEY)


def generate_sql_script(fix: SuggestedFix) -> str:
    """
    Generate SQL script for a fix suggestion.
    
    Uses Groq LLM to convert structured fix object into executable SQL.
    
    SAFETY CONSTRAINTS:
    - No DROP statements
    - No DELETE without WHERE clause
    - Includes comments and rollback instructions
    
    Args:
        fix: Structured fix suggestion
    
    Returns:
        SQL script as string
    
    Raises:
        ValueError: If GROQ_API_KEY not configured
    """
    client = _get_groq_client()
    
    # Build prompt with safety constraints
    prompt = f"""Generate a safe, executable SQL script for the following data fix:

Action: {fix.action}
Target: {fix.target}
Description: {fix.description}

SAFETY REQUIREMENTS:
- NO DROP statements
- NO DELETE without WHERE clause
- Include BEGIN/COMMIT transaction blocks
- Add comments explaining each step
- Include rollback instructions

Generate ONLY the SQL script, no additional explanation."""
    
    # Call Groq API
    response = client.chat.completions.create(
        model=settings.GROQ_MODEL,
        messages=[
            {
                "role": "system",
                "content": "You are a database expert generating safe SQL scripts. Always include transaction blocks and comments."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        temperature=0.1,  # Low temperature for deterministic output
        max_tokens=1000
    )
    
    sql_script = response.choices[0].message.content.strip()
    
    # Add header comment
    header = f"""-- Data Doctor: Automated Fix Script
-- Action: {fix.action}
-- Target: {fix.target}
-- Generated: {settings.APP_VERSION}
-- WARNING: Review this script before execution

"""
    
    return header + sql_script


def generate_markdown_report(diagnosis: DiagnosisResponse) -> str:
    """
    Generate formatted Markdown report.
    
    Uses Groq LLM to format the JSON diagnosis into readable Markdown.
    
    Args:
        diagnosis: Complete diagnosis response
    
    Returns:
        Markdown-formatted report
    
    Raises:
        ValueError: If GROQ_API_KEY not configured
    """
    client = _get_groq_client()
    
    # Build structured prompt
    prompt = f"""Convert this data incident diagnosis into a professional Markdown report:

INCIDENT ID: {diagnosis.incident_id}
TARGET ASSET: {diagnosis.target_asset}
SEVERITY: {diagnosis.severity}
CONFIDENCE: {diagnosis.confidence_score:.2f}

PRIMARY ROOT CAUSE:
{diagnosis.primary_root_cause.model_dump_json() if diagnosis.primary_root_cause else "None detected"}

CONTRIBUTING FACTORS:
{[f.model_dump() for f in diagnosis.contributing_factors]}

IMPACTED ASSETS:
- Tables: {len(diagnosis.impacted_assets.tables)}
- Dashboards: {len(diagnosis.impacted_assets.dashboards)}
- ML Models: {len(diagnosis.impacted_assets.ml_models)}
- Total Impact: {diagnosis.impacted_assets.total_impact_count}

SUGGESTED FIXES:
{[f.model_dump() for f in diagnosis.suggested_fixes]}

Create a professional Markdown report with:
1. Executive Summary
2. Root Cause Analysis
3. Impact Assessment
4. Recommended Actions
5. Next Steps

Use proper Markdown formatting with headers, lists, and emphasis."""
    
    response = client.chat.completions.create(
        model=settings.GROQ_MODEL,
        messages=[
            {
                "role": "system",
                "content": "You are a data engineering expert writing incident reports. Use clear, professional language and proper Markdown formatting."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        temperature=0.3,
        max_tokens=2000
    )
    
    return response.choices[0].message.content.strip()


def generate_slack_message(diagnosis: DiagnosisResponse) -> dict:
    """
    Generate Slack Block Kit message.
    
    Uses Groq LLM to format diagnosis into Slack Block Kit JSON.
    
    Args:
        diagnosis: Complete diagnosis response
    
    Returns:
        Slack Block Kit JSON
    
    Raises:
        ValueError: If GROQ_API_KEY not configured
    """
    client = _get_groq_client()
    
    # Determine severity color
    severity_colors = {
        Severity.HIGH: "#FF0000",  # Red
        Severity.MEDIUM: "#FFA500",  # Orange
        Severity.LOW: "#00FF00"  # Green
    }
    color = severity_colors.get(diagnosis.severity, "#808080")
    
    # Build prompt for Block Kit generation
    prompt = f"""Generate a Slack Block Kit JSON message for this data incident:

SEVERITY: {diagnosis.severity} (use color: {color})
TARGET: {diagnosis.target_asset}
CONFIDENCE: {diagnosis.confidence_score:.0%}

ROOT CAUSE: {diagnosis.primary_root_cause.name if diagnosis.primary_root_cause else "Unknown"} ({diagnosis.primary_root_cause.type if diagnosis.primary_root_cause else "N/A"})

IMPACT: {diagnosis.impacted_assets.total_impact_count} assets affected

FIXES: {len(diagnosis.suggested_fixes)} suggested actions

Create a Slack Block Kit JSON with:
1. Header section with severity emoji and title
2. Divider
3. Context section with target asset
4. Section with root cause details
5. Section with impact summary
6. Section with suggested fixes
7. Actions section with "View Details" button

Use proper Block Kit structure. Return ONLY valid JSON, no markdown code blocks."""
    
    response = client.chat.completions.create(
        model=settings.GROQ_MODEL,
        messages=[
            {
                "role": "system",
                "content": "You are a Slack integration expert. Generate valid Block Kit JSON following the official Slack API specification."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        temperature=0.1,
        max_tokens=1500
    )
    
    # Parse the JSON response
    import json
    import re
    block_kit_json = response.choices[0].message.content.strip()
    
    # Remove markdown code blocks if present - use robust regex
    json_match = re.search(r'```(?:json)?\s*([\s\S]+?)```', block_kit_json)
    if json_match:
        block_kit_json = json_match.group(1).strip()
    elif block_kit_json.startswith("```"):
        # Fallback to simple split if regex doesn't match
        block_kit_json = block_kit_json.split("```")[1]
        if block_kit_json.startswith("json"):
            block_kit_json = block_kit_json[4:]
        block_kit_json = block_kit_json.strip()
    
    return json.loads(block_kit_json)


def enhance_suggestions_with_ai(
    base_fixes: list[SuggestedFix],
    diagnosis: DiagnosisResponse
) -> list[SuggestedFix]:
    """
    Use AI to enhance deterministic suggestions with context-aware details.
    
    This function takes the deterministic base suggestions and uses AI to:
    1. Add context-specific details
    2. Suggest additional preventive actions
    3. Provide priority ordering based on impact
    4. Add warnings about potential side effects
    
    CRITICAL: AI enhances but does NOT replace deterministic suggestions.
    The base fixes are always included.
    
    Args:
        base_fixes: Deterministic suggestions from Phase 4
        diagnosis: Complete diagnosis with rich context
    
    Returns:
        Enhanced list of suggestions (includes base + AI additions)
    
    Raises:
        ValueError: If GROQ_API_KEY not configured
    """
    if not settings.GROQ_API_KEY:
        # Graceful degradation - return base fixes without enhancement
        return base_fixes
    
    try:
        client = _get_groq_client()
        
        # Build rich context prompt
        context = f"""You are a data engineering expert analyzing a data incident.

INCIDENT CONTEXT:
- Target Asset: {diagnosis.target_asset}
- Severity: {diagnosis.severity}
- Confidence: {diagnosis.confidence_score:.0%}
- Primary Root Cause: {diagnosis.primary_root_cause.type if diagnosis.primary_root_cause else "Unknown"} at {diagnosis.primary_root_cause.name if diagnosis.primary_root_cause else "Unknown"}
- Contributing Factors: {len(diagnosis.contributing_factors)} additional issues
- Impact: {diagnosis.impacted_assets.total_impact_count} downstream assets affected
  - Tables: {len(diagnosis.impacted_assets.tables)}
  - Dashboards: {len(diagnosis.impacted_assets.dashboards)}
  - ML Models: {len(diagnosis.impacted_assets.ml_models)}

DETERMINISTIC SUGGESTIONS (MUST KEEP):
{chr(10).join([f"- {fix.action}: {fix.description}" for fix in base_fixes])}

YOUR TASK:
1. Review the deterministic suggestions (these are correct and must be kept)
2. Add 1-2 ADDITIONAL context-aware suggestions based on:
   - The severity and impact
   - The specific root cause type
   - The downstream assets affected
3. For each suggestion, provide:
   - A specific action (be concrete, not generic)
   - Why it's important given this context
   - Any warnings or prerequisites

RULES:
- DO NOT remove or modify the deterministic suggestions
- DO NOT suggest dangerous operations (DROP, DELETE without WHERE)
- BE SPECIFIC to this incident (use the asset names, severity, impact)
- Focus on PREVENTION and MONITORING, not just fixes
- Keep suggestions actionable and realistic

Return your response as a JSON array of additional suggestions in this format:
[
  {{
    "action": "monitor_downstream",
    "target": "specific_asset_name",
    "description": "Specific action with context",
    "priority": "high|medium|low",
    "reasoning": "Why this is important given the context"
  }}
]

Return ONLY the JSON array, no markdown formatting."""

        response = client.chat.completions.create(
            model=settings.GROQ_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": "You are a data engineering expert providing context-aware incident response suggestions. Be specific and actionable."
                },
                {
                    "role": "user",
                    "content": context
                }
            ],
            temperature=0.3,  # Some creativity but still focused
            max_tokens=1000
        )
        
        # Parse AI response
        import json
        import re
        ai_response = response.choices[0].message.content.strip()
        
        # Remove markdown code blocks if present - use robust regex
        json_match = re.search(r'```(?:json)?\s*([\s\S]+?)```', ai_response)
        if json_match:
            ai_response = json_match.group(1).strip()
        elif ai_response.startswith("```"):
            # Fallback to simple split if regex doesn't match
            ai_response = ai_response.split("```")[1]
            if ai_response.startswith("json"):
                ai_response = ai_response[4:]
            ai_response = ai_response.strip()
        
        ai_suggestions = json.loads(ai_response)
        
        # Convert AI suggestions to SuggestedFix objects
        enhanced_fixes = base_fixes.copy()  # Start with deterministic fixes
        
        # Map AI action strings to FixAction enum
        action_mapping = {
            "monitor_downstream": FixAction.RERUN_PIPELINE,
            "setup_alerts": FixAction.RERUN_PIPELINE,
            "review_data": FixAction.QUARANTINE_DATA,
            "update_schema": FixAction.UPDATE_SCHEMA,
            "backfill": FixAction.FORCE_BACKFILL,
            "rerun": FixAction.RERUN_PIPELINE,
        }
        
        for ai_fix in ai_suggestions:
            # Try to map AI action to appropriate FixAction
            ai_action = ai_fix.get("action", "").lower()
            mapped_action = FixAction.RERUN_PIPELINE  # Default fallback
            
            for key, fix_action in action_mapping.items():
                if key in ai_action:
                    mapped_action = fix_action
                    break
            
            enhanced_fix = SuggestedFix(
                action=mapped_action,
                target=ai_fix.get("target", "system"),
                description=f"[AI-Enhanced] {ai_fix.get('description', '')} (Priority: {ai_fix.get('priority', 'medium')})",
                sql_script=None,
                markdown_details=ai_fix.get("reasoning")
            )
            enhanced_fixes.append(enhanced_fix)
        
        return enhanced_fixes
    
    except Exception as e:
        # If AI enhancement fails, return base fixes
        print(f"AI enhancement failed: {e}")
        return base_fixes


def send_slack_notification(diagnosis: DiagnosisResponse) -> bool:
    """
    Send diagnosis notification to Slack via webhook.
    
    Args:
        diagnosis: Complete diagnosis response
    
    Returns:
        True if sent successfully, False otherwise
    
    Raises:
        ValueError: If SLACK_WEBHOOK_URL not configured
    """
    if not settings.SLACK_WEBHOOK_URL:
        raise ValueError("SLACK_WEBHOOK_URL not configured in environment")
    
    try:
        # Generate Block Kit message
        blocks = generate_slack_message(diagnosis)
        
        # Send via webhook
        webhook = WebhookClient(settings.SLACK_WEBHOOK_URL)
        response = webhook.send(
            text=f"Data Doctor Alert: {diagnosis.severity} severity incident detected",
            blocks=blocks.get("blocks", [])
        )
        
        return response.status_code == 200
    
    except Exception as e:
        # Log error but don't fail the diagnosis
        print(f"Failed to send Slack notification: {e}")
        return False

