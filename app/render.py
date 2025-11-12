"""Email rendering: Markdown → HTML and MJML → HTML."""
import logging
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import markdown
from jinja2 import Template

from app.config import config

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "email_templates"


def markdown_to_html(md_content: str) -> str:
    """Convert Markdown to HTML with enhanced formatting for meeting cards."""
    import re

    # Log the first 500 chars of input to debug
    logger.info(f"Converting markdown, first 500 chars: {md_content[:500]}")

    # Remove any LLM-generated headers and replace with proper markdown headers
    # Clean up duplicate "Team Performance Table" text
    md_content = re.sub(r'\*\*##\s*TEAM PERFORMANCE TABLE\*\*', '', md_content)
    md_content = re.sub(r'##\s*\*\*Team Performance Table\*\*', '', md_content)

    # Convert plain text headers to proper markdown H2
    # Fix "🎯 ALL CONVERSATIONS (Best to Worst)" if LLM outputs it as plain text
    md_content = re.sub(r'^🎯\s*ALL CONVERSATIONS.*?\n', r'## 🎯 ALL CONVERSATIONS (Best to Worst)\n\n', md_content, flags=re.MULTILINE)

    # Add section headers if LLM didn't include them
    # Look for table or "ALL CONVERSATIONS" section
    if "📊 TEAM PERFORMANCE TABLE" not in md_content and "|" in md_content:
        # LLM output starts with table - add header
        md_content = "## 📊 TEAM PERFORMANCE TABLE\n\n" + md_content

    if "🎯 ALL CONVERSATIONS" not in md_content and "###" in md_content:
        # Add conversations header before first H3
        md_content = re.sub(r'(###)', r'## 🎯 ALL CONVERSATIONS (Best to Worst)\n\n\1', md_content, count=1)

    # Clean up H3 headers wrapped in bold - remove ** around the entire H3 line
    md_content = re.sub(r'###\s*\*\*(.*?)\*\*', r'### \1', md_content)

    # Convert markdown to HTML first with more extensions
    html = markdown.markdown(
        md_content,
        extensions=["extra", "nl2br", "sane_lists", "tables", "fenced_code", "attr_list"],
    )

    # Log the first 500 chars of HTML to see if conversion worked
    logger.info(f"After markdown conversion, first 500 chars: {html[:500]}")

    # Enhanced styling for client meeting cards
    # Pattern: <h3>Client Name</h3> followed by meeting details
    pattern = r'<h3>(.*?)</h3>(.*?)(?=<h3>|<h2>|$)'

    def replace_with_card(match):
        client_name = match.group(1).strip()
        content = match.group(2).strip()

        # Add card styling for client sections
        return f'''<div class="insight-card">
    <h3 style="color: #0066cc; margin-top: 0;">{client_name}</h3>
    <div style="padding-left: 10px;">
        {content}
    </div>
</div>'''

    # Apply card styling to h3 sections (client cards)
    html = re.sub(pattern, replace_with_card, html, flags=re.DOTALL)

    # Don't override strong styles - let CSS handle it
    # Remove inline styles that might interfere with CSS

    return html


def get_week_ending_date() -> str:
    """Get Friday of current week in DD MMM format (Europe/London)."""
    tz = ZoneInfo(config.TIMEZONE)
    now = datetime.now(tz)
    # Calculate days until Friday (weekday 4)
    days_ahead = 4 - now.weekday()
    if days_ahead < 0:  # Already past Friday
        days_ahead += 7
    friday = now + timedelta(days=days_ahead)
    return friday.strftime("%d %b")


def render_mjml_to_html(mjml_path: Path, content_html: str, total_meetings: int = None, days: int = 7) -> str:
    """
    Render MJML template to HTML.

    Uses npx mjml to convert MJML to HTML.
    Falls back to simple HTML template if mjml is not available.
    """
    try:
        # Read MJML template
        mjml_template = mjml_path.read_text()

        # Calculate date range
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(config.TIMEZONE)
        end_date = datetime.now(tz)
        start_date = end_date - timedelta(days=days)
        date_range = f"{start_date.strftime('%d %b')} - {end_date.strftime('%d %b %Y')}"

        # Replace placeholders
        mjml_with_content = mjml_template.replace("{{ content }}", content_html)
        mjml_with_content = mjml_with_content.replace("{{ current_date }}", end_date.strftime("%d %b %Y"))
        mjml_with_content = mjml_with_content.replace("{{ date_range }}", date_range)
        mjml_with_content = mjml_with_content.replace("{{ total_meetings }}", str(total_meetings or "N/A"))

        # Try to use npx mjml
        try:
            result = subprocess.run(
                ["npx", "mjml", "-s"],
                input=mjml_with_content.encode(),
                capture_output=True,
                timeout=10,
            )

            if result.returncode == 0:
                logger.info(f"Successfully rendered MJML template: {mjml_path.name}")
                return result.stdout.decode()
            else:
                logger.warning(f"MJML rendering failed, falling back to simple HTML: {result.stderr.decode()}")

        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            logger.warning(f"MJML tool not available ({e}), falling back to simple HTML")

    except Exception as e:
        logger.error(f"Error reading MJML template: {e}")

    # Fallback: simple HTML template
    return _render_simple_html(content_html, mjml_path.stem, total_meetings, days)


def _render_simple_html(content_html: str, mode: str, total_meetings: int = None, days: int = 7) -> str:
    """Simple HTML fallback template."""
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    title_map = {
        "insights": "Weekly Insights Report",
        "coaching": "Calls & Coaching Report",
    }
    subtitle = title_map.get(mode, "Weekly Update")

    # Calculate date range
    tz = ZoneInfo(config.TIMEZONE)
    end_date = datetime.now(tz)
    start_date = end_date - timedelta(days=days)
    date_range = f"{start_date.strftime('%d %b')} - {end_date.strftime('%d %b %Y')}"
    current_date = end_date.strftime("%d %b %Y")

    template = """<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>UNKNOWN Brain — {{ subtitle }}</title>
    <style>
        body {
            font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
            background-color: #f4f4f4;
            margin: 0;
            padding: 0;
        }
        .container {
            max-width: 600px;
            margin: 0 auto;
            background-color: #ffffff;
        }
        .header {
            text-align: center;
            padding: 30px 20px 10px;
        }
        .header h1 {
            font-size: 24px;
            font-weight: 700;
            margin: 0 0 10px 0;
            color: #1a1a1a;
        }
        .header .subtitle {
            font-size: 16px;
            color: #666666;
            margin: 0;
        }
        .content {
            padding: 20px 30px 30px;
            font-size: 15px;
            color: #333333;
            line-height: 1.7;
        }
        .content h2 {
            font-size: 20px;
            font-weight: 700;
            margin: 30px 0 15px 0;
            color: #1a1a1a;
            border-bottom: 2px solid #0066cc;
            padding-bottom: 8px;
        }
        .content h3 {
            font-size: 17px;
            font-weight: 700;
            margin: 25px 0 12px 0;
            color: #1a1a1a;
        }
        .content h4 {
            font-size: 15px;
            font-weight: 600;
            margin: 18px 0 8px 0;
            color: #2c2c2c;
        }
        .content ul {
            margin: 10px 0 20px 0;
            padding-left: 20px;
        }
        .content li {
            margin-bottom: 8px;
            line-height: 1.6;
        }
        .content p {
            margin: 0 0 12px 0;
        }
        .content strong, .content b {
            font-weight: 600;
            color: #1a1a1a;
        }
        .insight-card {
            background: #f8f9fa;
            border-left: 4px solid #0066cc;
            padding: 16px 20px;
            margin: 20px 0;
            border-radius: 4px;
        }
        .insight-card h3 {
            margin-top: 0;
            color: #0066cc;
            font-size: 16px;
        }
        .insight-card .evidence {
            font-style: italic;
            color: #555;
            margin: 8px 0;
            font-size: 14px;
        }
        .insight-card .tag {
            display: inline-block;
            background: #e3f2fd;
            color: #0066cc;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 600;
            margin-top: 8px;
        }
        .metric {
            font-size: 28px;
            font-weight: 700;
            color: #0066cc;
            line-height: 1.2;
        }
        .metric-label {
            font-size: 13px;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            font-weight: 600;
        }
        .content table {
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
        }
        .content th {
            background: #f8f8f8;
            padding: 10px;
            text-align: left;
            font-weight: 600;
            border-bottom: 2px solid #e0e0e0;
        }
        .content td {
            padding: 10px;
            border-bottom: 1px solid #f0f0f0;
        }
        .footer {
            font-size: 12px;
            color: #999999;
            margin-top: 30px;
            padding: 20px 30px 30px;
            border-top: 1px solid #e0e0e0;
            text-align: center;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>UNKNOWN Brain</h1>
            <p class="subtitle">{{ subtitle }}</p>
            <p style="font-size: 12px; color: #999; margin-top: 5px;">
                🤖 Automated Intelligence | Generated {{ current_date }}
            </p>
        </div>
        <div class="content">
            {{ content }}
        </div>
        <div class="footer">
            🤖 Automated UNKNOWN Brain Report<br/>
            Analysis Period: {{ date_range }}<br/>
            Generated from {{ total_meetings }} qualified meetings | Internal use only
        </div>
    </div>
</body>
</html>"""

    tmpl = Template(template)
    return tmpl.render(
        content=content_html,
        subtitle=subtitle,
        current_date=current_date,
        date_range=date_range,
        total_meetings=str(total_meetings or "N/A")
    )


def render_email(mode: str, markdown_content: str, total_meetings: int = None, days: int = 7) -> str:
    """
    Render complete email HTML from Markdown content.

    Args:
        mode: "insights" or "coaching"
        markdown_content: Markdown text from LLM
        total_meetings: Number of meetings analyzed
        days: Number of days in analysis period

    Returns:
        Complete HTML email
    """
    # Convert Markdown to HTML
    content_html = markdown_to_html(markdown_content)

    # Get MJML template path
    mjml_filename = f"{mode}.mjml"
    mjml_path = TEMPLATES_DIR / mjml_filename

    if not mjml_path.exists():
        logger.warning(f"Template not found: {mjml_path}, using fallback")
        return _render_simple_html(content_html, mode, total_meetings, days)

    # Render MJML to HTML
    return render_mjml_to_html(mjml_path, content_html, total_meetings, days)


def get_email_subject(mode: str) -> str:
    """Generate email subject line with week-ending date."""
    week_ending = get_week_ending_date()

    if mode == "insights":
        return f"UNKNOWN Brain — Weekly Team Performance (w/e {week_ending})"
    else:
        return f"UNKNOWN Brain — Calls & Coaching (w/e {week_ending})"
