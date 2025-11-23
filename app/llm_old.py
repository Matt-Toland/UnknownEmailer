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
| | Average | [X]/5 |
[Repeat for each UNKNOWN team member]
| Total: X conversations | Team Average | [X]/5 |

Include Average row after team members with multiple meetings.
Sort by highest average score first.

Data: {data}"""

# SECTION 3: CONVERSATION CARD PROMPT (for batches of 3)
CONVERSATION_CARD_PROMPT = """You are UNKNOWN's analyst. UNKNOWN helps companies hire creative talent and build freelance teams.

For EACH of these {count} CLIENT meetings, create a detailed analysis card:

### [Client Name] with [UNKNOWN Team Member]

**Meeting**: [Meeting Title] - [Date] | **Score**: [X]/5

✅ **What [Team Member] Uncovered:**

- **NOW**: [Quote EXACT evidence of client's immediate hiring needs, urgency, current pain points, timeline]
- **NEXT**: [Quote EXACT evidence of next steps in their hiring process, interview stages, decision timeline]
- **MEASURE**: [Quote EXACT evidence of how client will measure hiring success, KPIs, what good looks like]
- **BLOCKER**: [Quote EXACT evidence of obstacles to hiring, budget constraints, internal challenges]
- **FIT**: [Explain how this opportunity aligns with UNKNOWN's services - exec search, freelance bench, advisory]

💡 **Coaching for [Team Member]:**
[2-3 specific coaching points: questions to ask next time, areas to probe deeper about client needs, discovery techniques]

**Next Action**: [Specific follow-up with client - e.g., "Send three senior creative director profiles by Friday", "Clarify budget range for freelance bench", "Schedule chemistry meeting with shortlisted candidates"]

[View meeting →]({link})

---

CRITICAL: Quote client evidence VERBATIM. Focus on commercial opportunities. Be specific in coaching.

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
        """Generate all conversation cards using concurrent API calls (3 at a time)."""
        if not meetings:
            return "No qualified meetings found."
        
        # Sort meetings by score (best first)
        meetings_sorted = sorted(meetings, key=lambda x: x.get('score', 0), reverse=True)
        
        # Process in batches of 3 for concurrent calls
        batch_size = 3
        batches = [meetings_sorted[i:i+batch_size] for i in range(0, len(meetings_sorted), batch_size)]
        
        all_cards = []
        futures = []
        
        # Submit all batches for concurrent processing
        for batch in batches:
            future = self.executor.submit(self._generate_conversation_batch, batch)
            futures.append(future)
        
        # Collect results in order
        for future in futures:
            try:
                cards = future.result(timeout=30)  # 30 second timeout per batch
                all_cards.append(cards)
            except Exception as e:
                logger.error(f"Batch generation failed: {e}")
                # Generate fallback for this batch
                for meeting in batch:
                    all_cards.append(self._fallback_conversation_card(meeting))
        
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
                "link": meeting.get("meeting_link", meeting.get("granola_link", "#"))
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
        return f"""### {meeting.get('client', 'Unknown')} with {meeting.get('team_member', 'Unknown')}

**Meeting**: {meeting.get('title', 'Meeting')} - {meeting.get('date', 'Date')} | **Score**: {meeting.get('score', 0)}/5

✅ **What was uncovered:**
- **NOW**: {meeting.get('now_evidence', 'No immediate needs captured')[:200]}
- **NEXT**: {meeting.get('next_evidence', 'No next steps captured')[:200]}
- **MEASURE**: {meeting.get('measure_evidence', 'No success metrics captured')[:200]}
- **BLOCKER**: {meeting.get('blocker_evidence', 'No blockers identified')[:200]}
- **FIT**: Opportunity aligns with UNKNOWN's talent consultancy services

**Next Action**: Follow up with client on specific hiring requirements.

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