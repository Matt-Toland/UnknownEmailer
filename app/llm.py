"""OpenAI LLM integration and prompt templates."""
import json
import logging
from typing import Any, Dict, List

from openai import OpenAI

from app.config import config

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = """You are UNKNOWN's internal analyst. Write detailed, concrete UK-English weekly briefings. Be comprehensive and specific.

CRITICAL RULES:
1. Format output in Markdown with **double asterisks** for bold, ## for headings
2. QUOTE evidence verbatim - do not summarize or paraphrase
3. HIGHLIGHT all numbers: budgets (£/$), percentages (%), headcounts, timelines
4. Include Granola meeting links when available
5. Extract specific company names, role titles, and decision makers"""

INSIGHTS_USER_PROMPT = """Analyze UNKNOWN's weekly business intelligence. Create an executive briefing extracting maximum value from the evidence.


## 🎯 Priority Opportunities

For the top 3 meetings from now_pipeline, create detailed cards:

### [Client Name]
**Meeting**: "[meeting_title]" - [meeting_date]
**Owner**: [owner] | **Score**: [score]/5
**Key Numbers**: [Extract ALL budget/revenue/headcount numbers from evidence]

**Critical Evidence** (quote verbatim the most impactful parts):
> "[EXACT quote from now_evidence showing urgency/timeline]"
> "[EXACT quote from measure_evidence showing metrics/budget]"
> "[EXACT quote from blocker_evidence showing challenges]"

**Specific Challenges**: [List actual challenges from array if present]
**Next Action**: [Specific follow-up with date/person/topic based on evidence]
[View full notes →](meeting_link)

[Repeat for top 3 opportunities]

## 💰 Deal Pipeline

Extract ALL meetings mentioning specific budgets, revenue, or commercial numbers:
- List each with **client**, **amount**, and **context**
- Highlight urgency indicators (timelines, deadlines, competitive situations)

## 🎯 Active Talent Searches

Extract ALL hiring needs with:
- **Role**: [Exact title mentioned]
- **Budget**: [Salary/day rate if mentioned]
- **Timeline**: [Start date or urgency]
- **Client**: [Company name]

## 📊 Market Intelligence

From the evidence, extract:
- Industry trends (e.g., "Buy American policy", "AI replacing juniors")
- Competitive moves (companies mentioned, market shifts)
- Pricing benchmarks (day rates, project fees, salaries)

## 🔥 Hot Actions

Based on evidence urgency:
1. **IMMEDIATE**: [Client] needs [specific thing] by [date]
2. **THIS WEEK**: Follow up on [specific opportunity with context]
3. **PIPELINE**: Nurture [client] for [future opportunity]

REMEMBER: Quote evidence EXACTLY. Highlight ALL numbers. Include Granola links. Max 600 words.

Data:
{data}"""

COACHING_USER_PROMPT = """Create a team performance briefing focused on discovery quality and improvement.

IMPORTANT: Use proper Markdown formatting including **bold text** with double asterisks:

## 📊 Team Performance Summary
From summary data, show:
- Total meetings: [X] (vs last week: [+/-Y])
- Qualified rate: [X]% (vs last week: [+/-Y]%)
- Average score: [X] (vs last week: [+/-Y])

## 🏆 Top Performers
From host_performance data, recognize top 3:
Format: **[Name]** - Score: [X] | Discovery depth: [Y]% | Meetings: [Z]

## 💡 Individual Insights
For each host, identify their strongest and weakest discovery area:
- Strong at: [NOW/MEASURE/BLOCKER] ([X]% capture rate)
- Improve: [NOW/MEASURE/BLOCKER] ([Y]% capture rate)

## 🎯 Team Improvement Focus
From improvement_areas data, provide 2-3 specific actions:
- Current gap: [Metric at X%]
- Action: [Specific question to ask more]
- Target: [Y%]

## 📈 Quality Indicators
From team_benchmarks:
- Urgency discovery (NOW): [X]% of meetings
- Metrics discovery (MEASURE): [Y]% of meetings
- Blocker identification: [Z]% of meetings

Keep actionable and specific. Under 350 words.

Data:
{data}"""


class LLMClient:
    """OpenAI LLM client wrapper."""

    def __init__(self):
        self.client = OpenAI(api_key=config.OPENAI_API_KEY)
        self.model = config.DEFAULT_LLM_MODEL

    def generate_insights_v2(self, intelligence_data: Dict[str, Any]) -> str:
        """
        Generate business intelligence insights from comprehensive data.

        Returns:
            Markdown-formatted executive briefing
        """
        # KEEP ALL THE DATA - it was working with the original generate_insights method
        data_json = json.dumps(intelligence_data, indent=2, default=str)

        logger.info(f"Generating business intelligence insights using {self.model}")
        logger.info(f"Full data size: {len(data_json)} chars, Pipeline items: {len(intelligence_data.get('now_pipeline', []))}")

        # Build the full prompt with ALL data
        full_prompt = f"{SYSTEM_PROMPT}\n\n{INSIGHTS_USER_PROMPT.format(data=data_json)}"

        try:
            # The issue is the Responses API with gpt-5-mini
            # Let's use the SAME approach as the original generate_insights() which worked

            # First, try Responses API since that's what the model expects
            response = self.client.responses.create(
                model=self.model,
                input=full_prompt,
                reasoning={"effort": "minimal"},  # Back to minimal like original
                text={"verbosity": "low"},        # Back to low like original
                max_output_tokens=1500,
            )

            content = response.output_text or ""

            # If Responses API returns empty, fall back to Chat Completions
            if not content:
                logger.warning("Responses API returned empty, trying Chat Completions API...")

                # Use Chat Completions API like the original working version
                chat_response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": INSIGHTS_USER_PROMPT.format(data=data_json)}
                    ],
                    max_completion_tokens=2500,
                )

                content = chat_response.choices[0].message.content or ""
                logger.info(f"Chat Completions API returned: {len(content)} chars")

            else:
                logger.info(f"Responses API returned: {len(content)} chars")

            return content

        except Exception as e:
            logger.error(f"Primary generation failed: {e}")

            # Last resort: use the original generate_insights method that we know works
            logger.info("Falling back to original generate_insights method...")
            return self.generate_insights(intelligence_data.get("now_pipeline", [])[:25])

    def _create_basic_report(self, data: Dict[str, Any]) -> str:
        """Create a basic report when LLM fails."""
        report = "## 🎯 Priority Opportunities\n\n"

        for meeting in data.get("now_pipeline", [])[:3]:
            report += f"### {meeting.get('client', 'Unknown')}\n"
            report += f"- Date: {meeting.get('meeting_date', 'N/A')}\n"
            report += f"- Owner: {meeting.get('owner', 'N/A')}\n"
            report += f"- Score: {meeting.get('score', 0)}/5\n\n"

        summary = data.get("summary_metrics", {})
        report += f"\n## 📊 Weekly Performance\n"
        report += f"- Total Meetings: {summary.get('total_meetings', 0)}\n"
        report += f"- Qualified: {summary.get('pct_qualified', 0)}%\n"
        report += f"- Average Score: {summary.get('avg_score', 0)}\n"

        return report

    def generate_insights(self, meetings_data: List[Dict[str, Any]]) -> str:
        """
        Generate insights mode content from qualified meetings.

        Args:
            meetings_data: List of meeting dicts from BigQuery

        Returns:
            Markdown-formatted insights content
        """
        # Prepare simplified data for LLM
        simplified = []
        for m in meetings_data:
            item = {
                "meeting_id": m.get("meeting_id"),
                "date": str(m.get("date")) if m.get("date") else None,
                "desk": m.get("desk"),
                "title": m.get("title"),
                "score": m.get("total_qualified_sections", 0),
                "client_info": m.get("client_info"),
                "challenges": m.get("challenges", []),
                "results": m.get("results", []),
                "offering": m.get("offering"),
            }

            # Extract evidence from scoring JSON
            for criterion in ["now", "next", "measure", "blocker", "fit"]:
                criterion_data = m.get(criterion, {})
                if isinstance(criterion_data, dict):
                    evidence = criterion_data.get("evidence") or criterion_data.get("reasoning")
                    if evidence:
                        item[f"evidence_{criterion}"] = evidence

            simplified.append(item)

        data_json = json.dumps(simplified, indent=2, default=str)

        logger.info(f"Generating insights from {len(simplified)} meetings using {self.model}")

        # Build the prompt combining system and user messages
        full_prompt = f"{SYSTEM_PROMPT}\n\n{INSIGHTS_USER_PROMPT.format(data=data_json)}"

        try:
            # Use Responses API for gpt-5-mini
            response = self.client.responses.create(
                model=self.model,
                input=full_prompt,
                reasoning={"effort": "minimal"},
                text={"verbosity": "low"},
                max_output_tokens=1500,
            )

            content = response.output_text or ""
            logger.info(f"Generated insights: {len(content)} chars")
            return content

        except Exception as e:
            logger.error(f"LLM insights generation failed: {e}", exc_info=True)
            raise

    def generate_coaching_v2(self, coaching_data: Dict[str, Any]) -> str:
        """
        Generate team performance briefing from comprehensive coaching data.

        Args:
            coaching_data: Dict with host_performance, team_benchmarks,
                         trends, improvement_areas

        Returns:
            Markdown-formatted performance briefing
        """
        # Prepare data for LLM
        data_json = json.dumps(coaching_data, indent=2, default=str)

        logger.info(f"Generating team performance briefing using {self.model}")

        # Build the prompt
        full_prompt = f"{SYSTEM_PROMPT}\n\n{COACHING_USER_PROMPT.format(data=data_json)}"

        try:
            # Use Responses API for gpt-5-mini with higher verbosity
            response = self.client.responses.create(
                model=self.model,
                input=full_prompt,
                reasoning={"effort": "high"},  # Increased
                text={"verbosity": "high"},    # Increased
                max_output_tokens=3000,        # Increased
            )

            content = response.output_text or ""

            if len(content) < 1000:
                logger.warning(f"Generated coaching content too short: {len(content)} chars")
            else:
                logger.info(f"Generated coaching: {len(content)} chars")

            return content

        except Exception as e:
            logger.error(f"LLM coaching generation failed: {e}", exc_info=True)
            raise

    def generate_coaching(self, coaching_data: Dict[str, Any]) -> str:
        """
        Generate coaching mode content from aggregated data.

        Args:
            coaching_data: Dict with summary, leaderboard, signal_analysis, sample_meetings

        Returns:
            Markdown-formatted coaching content
        """
        # Simplify sample meetings
        simplified_samples = []
        for m in coaching_data.get("sample_meetings", []):
            item = {
                "meeting_id": m.get("meeting_id"),
                "date": str(m.get("date")) if m.get("date") else None,
                "desk": m.get("desk"),
                "title": m.get("title"),
                "score": m.get("total_qualified_sections", 0),
            }

            # Extract qualified status for each criterion
            for criterion in ["now", "next", "measure", "blocker", "fit"]:
                criterion_data = m.get(criterion, {})
                if isinstance(criterion_data, dict):
                    item[f"{criterion}_qualified"] = criterion_data.get("qualified", False)

            simplified_samples.append(item)

        # Build complete data structure
        data = {
            "summary": coaching_data.get("summary", {}),
            "leaderboard": coaching_data.get("leaderboard", []),
            "signal_analysis": coaching_data.get("signal_analysis", {}),
            "sample_meetings": simplified_samples,
        }

        data_json = json.dumps(data, indent=2, default=str)

        logger.info(f"Generating coaching summary using {self.model}")

        # Build the prompt combining system and user messages
        full_prompt = f"{SYSTEM_PROMPT}\n\n{COACHING_USER_PROMPT.format(data=data_json)}"

        try:
            # Use Responses API for gpt-5-mini
            response = self.client.responses.create(
                model=self.model,
                input=full_prompt,
                reasoning={"effort": "minimal"},
                text={"verbosity": "low"},
                max_output_tokens=1500,
            )

            content = response.output_text or ""
            logger.info(f"Generated coaching: {len(content)} chars")
            return content

        except Exception as e:
            logger.error(f"LLM coaching generation failed: {e}", exc_info=True)
            raise


# Lazy-loaded global instance
_llm_client = None


def get_llm_client() -> LLMClient:
    """Get or create LLM client instance."""
    global _llm_client
    if _llm_client is None:
        _llm_client = LLMClient()
    return _llm_client
