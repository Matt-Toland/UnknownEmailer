"""OpenAI LLM integration for UNKNOWN Brain client intelligence reports."""
import json
import logging
import asyncio
from typing import Any, Dict, List, Optional
from collections import defaultdict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from openai import OpenAI

from app.config import config

logger = logging.getLogger(__name__)

# Use GPT-4.1 for better quality
DEFAULT_MODEL = "gpt-4-1106-preview"  # or whatever GPT-4.1 model name is configured

# SECTION 1: EXECUTIVE SUMMARY PROMPT
EXECUTIVE_SUMMARY_PROMPT = """You are UNKNOWN's internal analyst. UNKNOWN is a creative talent consultancy that helps brands and agencies hire exceptional creative leaders and build flexible freelance teams.

Analyze this week's CLIENT meetings and write ONE comprehensive paragraph (150-200 words) for the Executive Summary covering:

1. Total qualified client meetings and average score
2. Top performer and their key client achievement (e.g., "Ellie led with a high-impact session with Instacart")
3. Key hiring trends observed across clients (flexibility on roles, freelance-to-perm, trial periods, contract structures)
4. Notable client patterns (e.g., "clients prioritizing adaptability over rigid titles", "emphasis on cultural fit")
5. Market signals and commercial opportunities (e.g., "broader trend toward agile hiring models", "emerging appetite for fractional executives")

Be specific. Reference actual clients. Highlight commercial opportunities.

Data: {data}"""

# SECTION 2: TEAM PERFORMANCE TABLE PROMPT
PERFORMANCE_TABLE_PROMPT = """Create a markdown table showing UNKNOWN team performance this week.

Format EXACTLY as:
| Name | Conversation | Score |
|------|--------------|-------|
| [Name] | [Client] — [Date] | [X]/5 |
| | [Client 2] — [Date 2] | [X]/5 |
| | **Average** | **[X.X]/5** |
| [Name 2] | [Client] — [Date] | [X]/5 |
| | **Average** | **[X.X]/5** |
| **Total: X conversations** | **Team Average** | **[X.X]/5** |

CRITICAL RULES:
1. List ALL conversations for each team member
2. ALWAYS include an Average row (in bold) after each team member's meetings
3. Sort team members by highest average score first
4. Use empty Name cell (|  |) for additional meetings and Average rows
5. Bold the Average rows and Total row

Data: {data}"""

# SECTION 3: CONVERSATION CARD PROMPT (for batches of 3)
CONVERSATION_CARD_PROMPT = """You are UNKNOWN's analyst. UNKNOWN helps companies hire creative talent and build freelance teams.

For EACH of these {count} CLIENT meetings, create a detailed analysis card:

### [Client Name] with [UNKNOWN Team Member]

**Meeting**: [Meeting Title] - [Date] | **Score**: [X]/5

✅ **What [Team Member] Uncovered:**

- **NOW**: [Synthesize immediate needs/current state in one flowing sentence - what's happening, why it matters, specific numbers/names]
- **NEXT**: [Synthesize future direction in one flowing sentence - where they're headed, decision timeline, next milestones]
- **MEASURE**: [Synthesize success metrics in one flowing sentence - how they'll evaluate, KPIs, timeframes]
- **BLOCKER**: [Synthesize obstacles in one flowing sentence - what's preventing progress, constraints, risks]
- **FIT**: [Explain UNKNOWN's specific value-add for this opportunity - which services (exec search/freelance/advisory), why now, commercial upside]

💡 **Coaching for [Team Member]:**
[2-3 hyper-specific questions to ask next that would unlock deal intelligence - e.g., "What would kill this deal post-LOI?", "Who's the internal skeptic?", "What's your integration playbook?" Focus on uncovering decision-making dynamics, hidden stakeholders, and deal-breakers]

**Next Action**: [Concrete deliverable with deadline and strategic intent - e.g., "Send comparative analysis of 3 candidates by Friday focusing on cultural fit markers", "Schedule follow-up Tuesday to discuss integration support needs"]

[View meeting →](meeting_link_here)

---

CRITICAL RULES:
1. SYNTHESIZE insights - DO NOT copy/paste raw evidence or use quotes
2. Write complete, flowing sentences with specific numbers, names, and details integrated naturally
3. Each bullet point should be ONE complete sentence (not fragments or lists)
4. Remove all quotation marks - rewrite everything in your own words
5. Coaching must be hyper-specific actionable questions (not generic "probe deeper")
6. Next Action must include concrete deliverable + deadline + strategic purpose
7. Focus on commercial value and deal intelligence
8. Replace "meeting_link_here" with the actual meeting_link from the meeting data

Meetings to analyze:
{meetings_data}"""

# SECTION 4: TEAM COACHING PROMPT
TEAM_COACHING_PROMPT = """Based on this week's client meetings, create the Team Coaching section:

## 🎯 TEAM COACHING

**What the team is doing well:**
[Identify 2-3 specific strengths in client discovery - e.g., "Strong at uncovering immediate hiring pain points (NOW)", "Excellent at identifying budget blockers early"]

**To become world-class:**
[2-3 specific improvements with examples:]
- For MEASURE: [e.g., "Ask clients 'What specific outcomes would make this hire successful in 90 days?'"]
- For FIT: [e.g., "Probe deeper on whether they need permanent hires vs. fractional/freelance solutions"]
- For BLOCKER: [e.g., "Uncover hidden stakeholders who might veto hiring decisions"]

**One thing to focus on:**
[Single most impactful improvement - e.g., "Always quantify the commercial impact of NOT hiring - what revenue/projects are at risk?"]

Data: {data}"""

class LLMClient:
    """OpenAI LLM client for UNKNOWN Brain client intelligence reports."""

    def __init__(self):
        self.client = OpenAI(api_key=config.OPENAI_API_KEY)
        self.model = config.DEFAULT_LLM_MODEL or DEFAULT_MODEL
        self.executor = ThreadPoolExecutor(max_workers=10)  # For concurrent API calls

    def generate_insights_v2(self, intelligence_data: Dict[str, Any]) -> str:
        """
        Generate complete UNKNOWN Brain report using separate LLM calls per section.
        
        Args:
            intelligence_data: BigQuery data with client meetings and stats
            
        Returns:
            Complete formatted report
        """
        logger.info(f"Generating UNKNOWN Brain report using {self.model}")
        
        # Process data for client meeting context
        processed_data = self._process_client_meetings(intelligence_data)
        
        # Generate each section separately
        sections = []
        
        # 1. Executive Summary
        logger.info("Generating Executive Summary...")
        executive_summary = self._generate_executive_summary(processed_data)
        sections.append(f"## 📊 EXECUTIVE SUMMARY\n\n{executive_summary}")
        
        # 2. Team Performance Table
        logger.info("Generating Team Performance Table...")
        performance_table = self._generate_performance_table(processed_data)
        sections.append(f"## 📊 TEAM PERFORMANCE TABLE\n\n{performance_table}")
        
        # 3. All Conversations (with concurrent processing)
        logger.info(f"Generating {len(processed_data['meetings'])} conversation cards...")
        conversation_cards = self._generate_all_conversations_concurrent(processed_data['meetings'])
        sections.append(f"## 🎯 ALL CONVERSATIONS (Best to Worst)\n\n{conversation_cards}")
        
        # 4. Team Coaching
        logger.info("Generating Team Coaching...")
        team_coaching = self._generate_team_coaching(processed_data)
        sections.append(team_coaching)
        
        # Combine all sections
        full_report = "\n\n".join(sections)
        
        logger.info(f"Complete report generated: {len(full_report)} chars")
        return full_report
    
    def _generate_executive_summary(self, data: Dict[str, Any]) -> str:
        """Generate executive summary section."""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are UNKNOWN's analyst for client intelligence."},
                    {"role": "user", "content": EXECUTIVE_SUMMARY_PROMPT.format(
                        data=json.dumps(data, indent=2, default=str)
                    )}
                ],
                max_tokens=500,
                temperature=0.3
            )
            return response.choices[0].message.content or self._fallback_executive_summary(data)
        except Exception as e:
            logger.error(f"Executive summary generation failed: {e}")
            return self._fallback_executive_summary(data)
    
    def _generate_performance_table(self, data: Dict[str, Any]) -> str:
        """Generate team performance table."""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Create a performance table for UNKNOWN team."},
                    {"role": "user", "content": PERFORMANCE_TABLE_PROMPT.format(
                        data=json.dumps(data['team_performance'], indent=2, default=str)
                    )}
                ],
                max_tokens=500,
                temperature=0.1  # Lower temp for structured output
            )
            return response.choices[0].message.content or self._fallback_performance_table(data)
        except Exception as e:
            logger.error(f"Performance table generation failed: {e}")
            return self._fallback_performance_table(data)
    
    def _generate_all_conversations_concurrent(self, meetings: List[Dict[str, Any]]) -> str:
        """Generate all conversation cards sequentially (more reliable than concurrent)."""
        if not meetings:
            return "No qualified meetings found."

        # Sort meetings by score (best first)
        meetings_sorted = sorted(meetings, key=lambda x: x.get('score', 0), reverse=True)

        # Process in batches of 3 meetings at a time
        batch_size = 3
        batches = [meetings_sorted[i:i+batch_size] for i in range(0, len(meetings_sorted), batch_size)]

        all_cards = []

        # Process each batch sequentially
        for batch in batches:
            try:
                logger.info(f"Generating batch of {len(batch)} conversation cards...")
                cards = self._generate_conversation_batch(batch)
                all_cards.append(cards)
            except Exception as e:
                logger.error(f"Batch generation failed: {e}")
                # Generate fallback for THIS specific batch
                fallback_cards = []
                for meeting in batch:
                    fallback_cards.append(self._fallback_conversation_card(meeting))
                all_cards.append("\n".join(fallback_cards))

        return "\n".join(all_cards)
    
    def _generate_conversation_batch(self, meetings_batch: List[Dict[str, Any]]) -> str:
        """Generate conversation cards for a batch of meetings."""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are UNKNOWN's analyst. Create detailed meeting analysis cards."},
                    {"role": "user", "content": CONVERSATION_CARD_PROMPT.format(
                        count=len(meetings_batch),
                        meetings_data=json.dumps(meetings_batch, indent=2, default=str)
                    )}
                ],
                max_tokens=2000,
                temperature=0.3
            )
            return response.choices[0].message.content or ""
        except Exception as e:
            logger.error(f"Conversation batch generation failed: {e}")
            # Return fallback cards for this batch
            return "\n---\n".join([self._fallback_conversation_card(m) for m in meetings_batch])
    
    def _generate_team_coaching(self, data: Dict[str, Any]) -> str:
        """Generate team coaching section."""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Generate coaching insights for UNKNOWN team."},
                    {"role": "user", "content": TEAM_COACHING_PROMPT.format(
                        data=json.dumps(data, indent=2, default=str)
                    )}
                ],
                max_tokens=800,
                temperature=0.4
            )
            return response.choices[0].message.content or self._fallback_team_coaching(data)
        except Exception as e:
            logger.error(f"Team coaching generation failed: {e}")
            return self._fallback_team_coaching(data)
    
    def _process_client_meetings(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process BigQuery data into client meeting format."""
        
        meetings = data.get("now_pipeline", []) or data.get("sample_meetings", [])
        
        processed_meetings = []
        team_stats = defaultdict(lambda: {"meetings": [], "total": 0, "count": 0})
        
        for meeting in meetings:
            # Extract client name
            client = meeting.get("client", "Unknown Client")
            if not client or client == "Unknown Client":
                client_info = meeting.get("client_info", {})
                if isinstance(client_info, dict):
                    client = client_info.get("client", "Unknown Client")
                elif isinstance(client_info, str):
                    try:
                        client = json.loads(client_info).get("client", "Unknown Client")
                    except:
                        pass
            
            # Extract UNKNOWN team member name
            team_member = meeting.get("owner", meeting.get("creator_name", "Unknown"))
            
            # Process meeting for client context
            processed_meeting = {
                "client": client,
                "team_member": team_member,
                "date": meeting.get("meeting_date", meeting.get("date", "")),
                "score": meeting.get("score", meeting.get("total_qualified_sections", 0)),
                "title": meeting.get("meeting_title", meeting.get("title", "Client Meeting")),
                "now_evidence": meeting.get("now_evidence", "") or self._extract_evidence(meeting, "now"),
                "next_evidence": meeting.get("next_evidence", "") or self._extract_evidence(meeting, "next"),
                "measure_evidence": meeting.get("measure_evidence", "") or self._extract_evidence(meeting, "measure"),
                "blocker_evidence": meeting.get("blocker_evidence", "") or self._extract_evidence(meeting, "blocker"),
                "fit_evidence": meeting.get("fit_evidence", "") or self._extract_evidence(meeting, "fit"),
                "challenges": meeting.get("challenges", []),
                "results": meeting.get("results", []),
                "offering": meeting.get("offering", ""),
                "meeting_link": meeting.get("meeting_link", meeting.get("granola_link", "#"))
            }
            
            processed_meetings.append(processed_meeting)
            
            # Update team member stats
            team_stats[team_member]["meetings"].append({
                "client": client,
                "date": processed_meeting["date"],
                "score": processed_meeting["score"]
            })
            team_stats[team_member]["total"] += processed_meeting["score"]
            team_stats[team_member]["count"] += 1
        
        # Calculate averages
        for member in team_stats:
            if team_stats[member]["count"] > 0:
                team_stats[member]["average"] = round(
                    team_stats[member]["total"] / team_stats[member]["count"], 1
                )
        
        # Calculate team stats
        total_meetings = len(processed_meetings)
        qualified_meetings = len([m for m in processed_meetings if m.get("score", 0) >= 3])
        average_score = round(
            sum(m.get("score", 0) for m in processed_meetings) / total_meetings, 1
        ) if total_meetings else 0
        
        return {
            "total_meetings": total_meetings,
            "qualified_meetings": qualified_meetings,
            "average_score": average_score,
            "team_performance": dict(team_stats),
            "meetings": processed_meetings,
            "summary": data.get("summary_metrics", {}),
            "trends": data.get("trends", {}),
            "client_patterns": data.get("client_concentration", {})
        }
    
    def _extract_evidence(self, meeting: Dict, criterion: str) -> str:
        """Extract evidence from meeting data."""
        criterion_data = meeting.get(criterion, {})
        if isinstance(criterion_data, dict):
            return criterion_data.get("evidence", criterion_data.get("reasoning", ""))
        return ""
    
    # Fallback methods for when LLM fails
    def _fallback_executive_summary(self, data: Dict[str, Any]) -> str:
        return f"The team delivered {data.get('qualified_meetings', 0)} qualified client meetings this period, maintaining a {data.get('average_score', 0)}/5 average score. Key opportunities identified across technology and creative sectors."
    
    def _fallback_performance_table(self, data: Dict[str, Any]) -> str:
        table = "| Name | Meetings | Avg Score |\n|------|----------|----------|\n"
        for member, stats in data.get('team_performance', {}).items():
            table += f"| {member} | {stats['count']} | {stats.get('average', 0)}/5 |\n"
        table += f"| **Total: {data.get('total_meetings', 0)} conversations** | **Team Average** | **{data.get('average_score', 0)}/5** |"
        return table
    
    def _fallback_conversation_card(self, meeting: Dict[str, Any]) -> str:
        link = meeting.get('meeting_link', '#')
        client = meeting.get('client', 'Unknown')
        team_member = meeting.get('team_member', 'Unknown')

        # Synthesize evidence into flowing sentences
        now = meeting.get('now_evidence', '')[:150] if meeting.get('now_evidence') else 'Client needs assessment in progress'
        next_step = meeting.get('next_evidence', '')[:150] if meeting.get('next_evidence') else 'Decision timeline to be confirmed'
        measure = meeting.get('measure_evidence', '')[:150] if meeting.get('measure_evidence') else 'Success criteria to be defined'
        blocker = meeting.get('blocker_evidence', '')[:150] if meeting.get('blocker_evidence') else 'No immediate blockers identified'

        return f"""### {client} with {team_member}

**Meeting**: {meeting.get('title', 'Meeting')} - {meeting.get('date', 'Date')} | **Score**: {meeting.get('score', 0)}/5

✅ **What {team_member} Uncovered:**

- **NOW**: {now}
- **NEXT**: {next_step}
- **MEASURE**: {measure}
- **BLOCKER**: {blocker}
- **FIT**: Strategic opportunity for UNKNOWN's talent advisory and placement services with immediate commercial upside

💡 **Coaching for {team_member}:**
Dig deeper on decision authority and timeline - ask who the final decision-maker is, what would accelerate the process, and whether there are competing internal priorities that could derail progress.

**Next Action**: Schedule follow-up meeting to clarify specific role requirements and confirm next steps with concrete deliverables and deadlines.

[View meeting →]({link})

---"""
    
    def _fallback_team_coaching(self, data: Dict[str, Any]) -> str:
        return """## 🎯 TEAM COACHING

**What the team is doing well:**
Maintaining consistent client meeting quality with good discovery depth.

**To become world-class:**
- For MEASURE: Ask clients "What specific outcomes would make this hire successful in 90 days?"
- For FIT: Clarify whether clients need permanent hires vs. fractional/freelance solutions

**One thing to focus on:**
Always quantify the commercial impact of not hiring - what revenue or projects are at risk without the right talent?"""
    
    def __del__(self):
        """Cleanup executor on deletion."""
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=False)


# Lazy-loaded global instance
_llm_client = None


def get_llm_client() -> LLMClient:
    """Get or create LLM client instance."""
    global _llm_client
    if _llm_client is None:
        _llm_client = LLMClient()
    return _llm_client