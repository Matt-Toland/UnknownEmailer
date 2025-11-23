"""Analytics module for trend calculations and business intelligence."""
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

from google.cloud import bigquery

from app.config import config

logger = logging.getLogger(__name__)


class AnalyticsClient:
    """Analytics client for advanced BigQuery queries and trend analysis."""

    def __init__(self):
        self.client = bigquery.Client(project=config.BQ_PROJECT_ID)
        self.table_id = config.get_full_table_id()
        self.tz = ZoneInfo(config.TIMEZONE)

    def get_now_pipeline(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        Fetch top scoring qualified meetings, prioritizing highest scores.

        Since all meetings have NOW=true in this dataset, we focus on highest scores.
        """
        end_date = datetime.now(self.tz)
        # Set start_date to beginning of day to capture all meetings from that date
        start_date = (end_date - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)

        query = f"""
        SELECT
            meeting_id,
            FORMAT_DATE('%d %b %Y', date) as meeting_date,
            IFNULL(JSON_VALUE(client_info, '$.client'), 'Unknown Client') as client,
            creator_name as owner,
            creator_email,
            total_qualified_sections as score,
            JSON_VALUE(now, '$.summary') as urgency_signal,
            JSON_VALUE(now, '$.evidence') as now_evidence,
            JSON_VALUE(next, '$.evidence') as next_evidence,
            JSON_VALUE(measure, '$.evidence') as measure_evidence,
            JSON_VALUE(blocker, '$.evidence') as blocker_evidence,
            JSON_VALUE(fit, '$.evidence') as fit_evidence,
            -- Qualification flags for coaching feedback
            JSON_VALUE(now, '$.qualified') as now_qualified,
            JSON_VALUE(next, '$.qualified') as next_qualified,
            JSON_VALUE(measure, '$.qualified') as measure_qualified,
            JSON_VALUE(blocker, '$.qualified') as blocker_qualified,
            JSON_VALUE(fit, '$.qualified') as fit_qualified,
            IFNULL(title, calendar_event_title) as meeting_title,
            challenges,
            results,
            offering,
            -- Desk/department for context
            desk,
            -- Meeting link if available
            IFNULL(granola_link, '') as meeting_link
        FROM `{self.table_id}`
        WHERE TIMESTAMP(date) BETWEEN @start_date AND @end_date
            AND qualified = TRUE
        ORDER BY total_qualified_sections DESC, date DESC
        LIMIT 30
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date),
                bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date),
            ]
        )

        logger.info(f"Fetching NOW pipeline for last {days} days")

        try:
            query_job = self.client.query(query, job_config=job_config)
            results = []
            for row in query_job.result():
                results.append(dict(row.items()))

            logger.info(f"Found {len(results)} urgent opportunities")
            return results
        except Exception as e:
            logger.error(f"NOW pipeline query failed: {e}", exc_info=True)
            return []

    def get_client_concentration(self, days: int = 7) -> Dict[str, Any]:
        """
        Analyze client engagement patterns.

        Returns top clients, new vs returning ratio, and engagement depth.
        """
        end_date = datetime.now(self.tz)
        # Set start_date to beginning of day to capture all meetings from that date
        start_date = (end_date - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)

        query = f"""
        WITH client_stats AS (
            SELECT
                JSON_VALUE(client_info, '$.client') as client,
                COUNT(*) as meeting_count,
                AVG(total_qualified_sections) as avg_score,
                COUNTIF(qualified = TRUE) as qualified_count,
                MAX(total_qualified_sections) as best_score,
                -- Get most recent meeting details
                ARRAY_AGG(
                    STRUCT(
                        FORMAT_DATE('%d %b', date) as meeting_date,
                        creator_name as owner,
                        title as meeting_title
                    )
                    ORDER BY date DESC
                    LIMIT 1
                )[OFFSET(0)] as latest_meeting
            FROM `{self.table_id}`
            WHERE TIMESTAMP(date) BETWEEN @start_date AND @end_date
                AND JSON_VALUE(client_info, '$.client') IS NOT NULL
            GROUP BY client
        )
        SELECT
            client,
            meeting_count,
            ROUND(avg_score, 1) as avg_score,
            qualified_count,
            best_score,
            latest_meeting.meeting_date as last_meeting_date,
            latest_meeting.owner as last_meeting_owner,
            latest_meeting.meeting_title as last_meeting_title
        FROM client_stats
        ORDER BY meeting_count DESC, avg_score DESC
        LIMIT 10
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date),
                bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date),
            ]
        )

        try:
            query_job = self.client.query(query, job_config=job_config)
            results = [dict(row.items()) for row in query_job.result()]

            return {
                "top_clients": results,
                "unique_clients": len(results),
            }
        except Exception as e:
            logger.error(f"Client concentration query failed: {e}", exc_info=True)
            return {"top_clients": [], "unique_clients": 0}

    def get_service_fit_analysis(self, days: int = 7) -> Dict[str, Any]:
        """
        Analyze service alignment (Access/Transform/Ventures).

        Returns distribution of opportunities by service type.
        """
        end_date = datetime.now(self.tz)
        # Set start_date to beginning of day to capture all meetings from that date
        start_date = (end_date - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)

        query = f"""
        SELECT
            JSON_VALUE(fit, '$.services[0]') as primary_service,
            COUNT(*) as count,
            AVG(total_qualified_sections) as avg_score
        FROM `{self.table_id}`
        WHERE TIMESTAMP(date) BETWEEN @start_date AND @end_date
            AND qualified = TRUE
            AND JSON_VALUE(fit, '$.qualified') = 'true'
        GROUP BY primary_service
        ORDER BY count DESC
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date),
                bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date),
            ]
        )

        try:
            query_job = self.client.query(query, job_config=job_config)
            results = []
            total = 0
            for row in query_job.result():
                row_dict = dict(row.items())
                results.append(row_dict)
                total += row_dict.get("count", 0)

            # Calculate percentages
            for item in results:
                item["percentage"] = round(100.0 * item["count"] / total, 1) if total > 0 else 0
                item["avg_score"] = round(item.get("avg_score", 0), 1)

            return {
                "service_distribution": results,
                "total_fit_opportunities": total,
            }
        except Exception as e:
            logger.error(f"Service fit analysis failed: {e}", exc_info=True)
            return {"service_distribution": [], "total_fit_opportunities": 0}

    def get_deal_pipeline(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        Extract meetings with specific budget/revenue mentions.

        Returns meetings with commercial numbers for deal tracking.
        """
        end_date = datetime.now(self.tz)
        # Set start_date to beginning of day to capture all meetings from that date
        start_date = (end_date - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)

        query = f"""
        SELECT
            IFNULL(JSON_VALUE(client_info, '$.client'), 'Unknown Client') as client,
            IFNULL(title, calendar_event_title) as meeting_title,
            FORMAT_DATE('%d %b %Y', date) as meeting_date,
            creator_name as owner,
            -- Extract all evidence that might contain numbers
            JSON_VALUE(now, '$.evidence') as now_evidence,
            JSON_VALUE(measure, '$.evidence') as measure_evidence,
            JSON_VALUE(blocker, '$.evidence') as blocker_evidence,
            -- Granola link for full context
            IFNULL(granola_link, '') as meeting_link
        FROM `{self.table_id}`
        WHERE TIMESTAMP(date) BETWEEN @start_date AND @end_date
            AND qualified = TRUE
            AND (
                REGEXP_CONTAINS(JSON_VALUE(now, '$.evidence'), r'[£$][0-9]') OR
                REGEXP_CONTAINS(JSON_VALUE(measure, '$.evidence'), r'[£$][0-9]') OR
                REGEXP_CONTAINS(JSON_VALUE(blocker, '$.evidence'), r'[£$][0-9]') OR
                REGEXP_CONTAINS(JSON_VALUE(now, '$.evidence'), r'\d+[kK]') OR
                REGEXP_CONTAINS(JSON_VALUE(measure, '$.evidence'), r'\d+[kK]') OR
                REGEXP_CONTAINS(JSON_VALUE(blocker, '$.evidence'), r'\d+[kK]')
            )
        ORDER BY scored_at DESC
        LIMIT 30
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date),
                bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date),
            ]
        )

        try:
            logger.info(f"Fetching deal pipeline with budget mentions")
            results = self.client.query(query, job_config=job_config).result()

            deals = []
            for row in results:
                # Extract all numbers from evidence
                all_evidence = f"{row.now_evidence or ''} {row.measure_evidence or ''} {row.blocker_evidence or ''}"

                deals.append({
                    "client": row.client,
                    "meeting_title": row.meeting_title,
                    "meeting_date": row.meeting_date,
                    "owner": row.owner,
                    "evidence_with_numbers": all_evidence[:500],  # Truncate for readability
                    "meeting_link": row.meeting_link,
                })

            logger.info(f"Found {len(deals)} meetings with budget/revenue mentions")
            return deals

        except Exception as e:
            logger.error(f"Deal pipeline extraction failed: {e}", exc_info=True)
            return []

    def get_blocker_patterns(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        Extract common blockers preventing deals.

        Returns top blockers with evidence and frequency.
        """
        end_date = datetime.now(self.tz)
        # Set start_date to beginning of day to capture all meetings from that date
        start_date = (end_date - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)

        query = f"""
        SELECT
            JSON_VALUE(blocker, '$.summary') as blocker_type,
            JSON_VALUE(blocker, '$.evidence') as evidence,
            COUNT(*) as frequency
        FROM `{self.table_id}`
        WHERE TIMESTAMP(date) BETWEEN @start_date AND @end_date
            AND JSON_VALUE(blocker, '$.qualified') = 'true'
        GROUP BY blocker_type, evidence
        ORDER BY frequency DESC
        LIMIT 5
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date),
                bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date),
            ]
        )

        try:
            query_job = self.client.query(query, job_config=job_config)
            results = [dict(row.items()) for row in query_job.result()]
            logger.info(f"Found {len(results)} blocker patterns")
            return results
        except Exception as e:
            logger.error(f"Blocker patterns query failed: {e}", exc_info=True)
            return []

    def get_host_performance(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        Calculate individual host performance with discovery depth metrics.

        Returns per-host metrics including quality indicators.
        """
        end_date = datetime.now(self.tz)
        # Set start_date to beginning of day to capture all meetings from that date
        start_date = (end_date - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)

        query = f"""
        SELECT
            creator_email,
            creator_name,
            COUNT(*) as total_meetings,
            COUNTIF(qualified = TRUE) as qualified_meetings,
            ROUND(AVG(total_qualified_sections), 1) as avg_score,

            -- Discovery depth metrics
            ROUND(100.0 * AVG(CASE WHEN JSON_VALUE(now, '$.qualified') = 'true' THEN 1 ELSE 0 END), 1) as now_rate,
            ROUND(100.0 * AVG(CASE WHEN JSON_VALUE(next, '$.qualified') = 'true' THEN 1 ELSE 0 END), 1) as next_rate,
            ROUND(100.0 * AVG(CASE WHEN JSON_VALUE(measure, '$.qualified') = 'true' THEN 1 ELSE 0 END), 1) as measure_rate,
            ROUND(100.0 * AVG(CASE WHEN JSON_VALUE(blocker, '$.qualified') = 'true' THEN 1 ELSE 0 END), 1) as blocker_rate,
            ROUND(100.0 * AVG(CASE WHEN JSON_VALUE(fit, '$.qualified') = 'true' THEN 1 ELSE 0 END), 1) as fit_rate,

            -- Quality score (average of discovery rates)
            ROUND(100.0 * AVG(
                (CASE WHEN JSON_VALUE(now, '$.qualified') = 'true' THEN 1 ELSE 0 END +
                 CASE WHEN JSON_VALUE(measure, '$.qualified') = 'true' THEN 1 ELSE 0 END +
                 CASE WHEN JSON_VALUE(blocker, '$.qualified') = 'true' THEN 1 ELSE 0 END) / 3.0
            ), 1) as discovery_depth_score

        FROM `{self.table_id}`
        WHERE TIMESTAMP(date) BETWEEN @start_date AND @end_date
            AND creator_email IS NOT NULL
        GROUP BY creator_email, creator_name
        HAVING total_meetings >= 2  -- Only show hosts with multiple meetings
        ORDER BY avg_score DESC
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date),
                bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date),
            ]
        )

        try:
            query_job = self.client.query(query, job_config=job_config)
            results = [dict(row.items()) for row in query_job.result()]

            # Calculate team averages for benchmarking
            if results:
                team_avg_score = sum(r["avg_score"] for r in results) / len(results)
                team_avg_discovery = sum(r["discovery_depth_score"] for r in results) / len(results)

                for host in results:
                    host["vs_team_score"] = round(host["avg_score"] - team_avg_score, 1)
                    host["vs_team_discovery"] = round(host["discovery_depth_score"] - team_avg_discovery, 1)

            logger.info(f"Analyzed performance for {len(results)} hosts")
            return results
        except Exception as e:
            logger.error(f"Host performance query failed: {e}", exc_info=True)
            return []

    def get_week_over_week_trends(self) -> Dict[str, Any]:
        """
        Calculate week-over-week trends for key metrics.

        Returns comparison of this week vs last week.
        """
        end_date = datetime.now(self.tz)
        # Set week boundaries to beginning of day
        this_week_start = (end_date - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
        last_week_start = (end_date - timedelta(days=14)).replace(hour=0, minute=0, second=0, microsecond=0)

        query = f"""
        WITH this_week AS (
            SELECT
                COUNT(*) as meetings,
                COUNTIF(qualified = TRUE) as qualified,
                AVG(total_qualified_sections) as avg_score
            FROM `{self.table_id}`
            WHERE TIMESTAMP(date) BETWEEN @this_week_start AND @end_date
        ),
        last_week AS (
            SELECT
                COUNT(*) as meetings,
                COUNTIF(qualified = TRUE) as qualified,
                AVG(total_qualified_sections) as avg_score
            FROM `{self.table_id}`
            WHERE TIMESTAMP(date) BETWEEN @last_week_start AND @this_week_start
        )
        SELECT
            tw.meetings as this_week_meetings,
            tw.qualified as this_week_qualified,
            ROUND(tw.avg_score, 1) as this_week_avg_score,
            lw.meetings as last_week_meetings,
            lw.qualified as last_week_qualified,
            ROUND(lw.avg_score, 1) as last_week_avg_score
        FROM this_week tw, last_week lw
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date),
                bigquery.ScalarQueryParameter("this_week_start", "TIMESTAMP", this_week_start),
                bigquery.ScalarQueryParameter("last_week_start", "TIMESTAMP", last_week_start),
            ]
        )

        try:
            query_job = self.client.query(query, job_config=job_config)
            results = list(query_job.result())

            if results:
                data = dict(results[0].items())

                # Calculate deltas
                trends = {
                    "this_week": {
                        "meetings": data.get("this_week_meetings", 0),
                        "qualified": data.get("this_week_qualified", 0),
                        "avg_score": data.get("this_week_avg_score", 0),
                    },
                    "last_week": {
                        "meetings": data.get("last_week_meetings", 0),
                        "qualified": data.get("last_week_qualified", 0),
                        "avg_score": data.get("last_week_avg_score", 0),
                    },
                    "deltas": {
                        "meetings_change": data.get("this_week_meetings", 0) - data.get("last_week_meetings", 0),
                        "qualified_change": data.get("this_week_qualified", 0) - data.get("last_week_qualified", 0),
                        "score_change": round(
                            data.get("this_week_avg_score", 0) - data.get("last_week_avg_score", 0), 1
                        ),
                    },
                }

                # Calculate percentage changes
                if data.get("last_week_meetings", 0) > 0:
                    trends["deltas"]["meetings_pct"] = round(
                        100.0 * trends["deltas"]["meetings_change"] / data["last_week_meetings"], 1
                    )
                if data.get("last_week_qualified", 0) > 0:
                    trends["deltas"]["qualified_pct"] = round(
                        100.0 * trends["deltas"]["qualified_change"] / data["last_week_qualified"], 1
                    )

                return trends

            return {
                "this_week": {"meetings": 0, "qualified": 0, "avg_score": 0},
                "last_week": {"meetings": 0, "qualified": 0, "avg_score": 0},
                "deltas": {"meetings_change": 0, "qualified_change": 0, "score_change": 0},
            }

        except Exception as e:
            logger.error(f"Week-over-week trends query failed: {e}", exc_info=True)
            return {
                "this_week": {},
                "last_week": {},
                "deltas": {},
            }

    def get_team_performance_table(self, days: int = 7) -> Dict[str, Any]:
        """
        Get team performance table with all meetings grouped by person.

        Returns meetings for each person with their scores and averages.
        """
        end_date = datetime.now(self.tz)
        # Set start_date to beginning of day to capture all meetings from that date
        start_date = (end_date - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)

        query = f"""
        SELECT
            creator_name as person_name,
            IFNULL(title, calendar_event_title) as conversation_title,
            total_qualified_sections as score,
            FORMAT_DATE('%d %b', date) as date_short
        FROM `{self.table_id}`
        WHERE TIMESTAMP(date) BETWEEN @start_date AND @end_date
            AND qualified = TRUE
        ORDER BY creator_name, total_qualified_sections DESC, date DESC
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date),
                bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date),
            ]
        )

        try:
            logger.info(f"Fetching team performance table for last {days} days")
            results = self.client.query(query, job_config=job_config).result()

            # Group by person and calculate averages
            people_data = {}
            for row in results:
                person = row.person_name
                if person not in people_data:
                    people_data[person] = {
                        "person_name": person,
                        "conversations": [],
                        "total_score": 0,
                        "count": 0
                    }

                people_data[person]["conversations"].append({
                    "title": row.conversation_title,
                    "score": row.score,
                    "date": row.date_short
                })
                people_data[person]["total_score"] += row.score
                people_data[person]["count"] += 1

            # Calculate averages
            for person in people_data.values():
                person["average_score"] = round(person["total_score"] / person["count"], 1) if person["count"] > 0 else 0

            # Convert to list and sort by person name
            team_list = list(people_data.values())
            team_list.sort(key=lambda x: x["person_name"])

            logger.info(f"Found {len(team_list)} people with {sum(p['count'] for p in team_list)} total conversations")

            return {
                "people": team_list,
                "total_conversations": sum(p["count"] for p in team_list),
                "team_average": round(sum(p["total_score"] for p in team_list) / sum(p["count"] for p in team_list), 1) if team_list else 0
            }

        except Exception as e:
            logger.error(f"Team performance table query failed: {e}", exc_info=True)
            return {"people": [], "total_conversations": 0, "team_average": 0}


# Lazy-loaded global instance
_analytics_client = None


def get_analytics_client() -> AnalyticsClient:
    """Get or create analytics client instance."""
    global _analytics_client
    if _analytics_client is None:
        _analytics_client = AnalyticsClient()
    return _analytics_client