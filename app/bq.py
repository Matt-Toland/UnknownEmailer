"""BigQuery helpers for fetching meeting intelligence data."""
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

from google.cloud import bigquery

from app.config import config

logger = logging.getLogger(__name__)


class BigQueryClient:
    """BigQuery client wrapper."""

    def __init__(self):
        self.client = bigquery.Client(project=config.BQ_PROJECT_ID)
        self.table_id = config.get_full_table_id()

    def _get_date_window(self, days: int = 7) -> tuple[datetime, datetime]:
        """Get date window for queries in Europe/London timezone."""
        tz = ZoneInfo(config.TIMEZONE)
        now = datetime.now(tz)
        end_date = now.replace(hour=23, minute=59, second=59)
        start_date = end_date - timedelta(days=days)
        return start_date, end_date

    def fetch_insights_data_v2(self, days: int = 7) -> Dict[str, Any]:
        """
        Fetch comprehensive business intelligence data for insights mode.

        Returns structured data with NOW pipeline, client intelligence,
        service patterns, and blockers.
        """
        from app.analytics import get_analytics_client
        analytics = get_analytics_client()

        # Gather all intelligence data
        return {
            "team_performance": analytics.get_team_performance_table(days),
            "now_pipeline": analytics.get_now_pipeline(days),
            "client_concentration": analytics.get_client_concentration(days),
            "service_fit": analytics.get_service_fit_analysis(days),
            "blocker_patterns": analytics.get_blocker_patterns(days),
            "trends": analytics.get_week_over_week_trends(),
            "summary_metrics": self._get_summary_metrics(days),
        }

    def _get_summary_metrics(self, days: int = 7) -> Dict[str, Any]:
        """Get basic summary metrics for the period."""
        start_date, end_date = self._get_date_window(days)

        query = f"""
        SELECT
            COUNT(*) as total_meetings,
            COUNTIF(qualified = TRUE) as qualified_meetings,
            ROUND(AVG(total_qualified_sections), 1) as avg_score,
            ROUND(100.0 * COUNTIF(qualified = TRUE) / COUNT(*), 1) as pct_qualified
        FROM `{self.table_id}`
        WHERE scored_at BETWEEN @start_date AND @end_date
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date),
                bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date),
            ]
        )

        try:
            query_job = self.client.query(query, job_config=job_config)
            results = list(query_job.result())
            return dict(results[0].items()) if results else {}
        except Exception as e:
            logger.error(f"Summary metrics query failed: {e}", exc_info=True)
            return {}

    def fetch_insights_data(self, days: int = 7, limit: int = 25) -> List[Dict[str, Any]]:
        """
        Fetch qualified meetings for insights mode.

        Returns top meetings by total_qualified_sections, ordered by score DESC.
        """
        start_date, end_date = self._get_date_window(days)

        query = f"""
        SELECT
            meeting_id,
            date,
            participants,
            desk,
            title,
            client_info,
            total_qualified_sections,
            now,
            next,
            measure,
            blocker,
            fit,
            challenges,
            results,
            offering,
            scored_at
        FROM `{self.table_id}`
        WHERE
            scored_at BETWEEN @start_date AND @end_date
            AND qualified = TRUE
        ORDER BY total_qualified_sections DESC, scored_at DESC
        LIMIT @limit
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date),
                bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date),
                bigquery.ScalarQueryParameter("limit", "INT64", limit),
            ]
        )

        logger.info(
            f"Fetching insights data: {start_date.isoformat()} to {end_date.isoformat()}, limit={limit}"
        )

        try:
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()

            rows = []
            for row in results:
                row_dict = dict(row.items())
                # Convert JSON fields to dict if they're strings
                if isinstance(row_dict.get("client_info"), str):
                    import json
                    try:
                        row_dict["client_info"] = json.loads(row_dict["client_info"])
                    except (json.JSONDecodeError, TypeError):
                        pass

                # Parse JSON scoring fields
                for field in ["now", "next", "measure", "blocker", "fit"]:
                    if isinstance(row_dict.get(field), str):
                        try:
                            row_dict[field] = json.loads(row_dict[field])
                        except (json.JSONDecodeError, TypeError):
                            pass

                rows.append(row_dict)

            logger.info(f"Fetched {len(rows)} qualified meetings for insights")
            return rows

        except Exception as e:
            logger.error(f"BigQuery insights query failed: {e}", exc_info=True)
            raise

    def fetch_coaching_data_v2(self, days: int = 7) -> Dict[str, Any]:
        """
        Fetch comprehensive coaching data with individual performance metrics.

        Returns host performance, team benchmarks, and improvement areas.
        """
        from app.analytics import get_analytics_client
        analytics = get_analytics_client()

        # Get host performance metrics
        host_performance = analytics.get_host_performance(days)

        # Calculate team benchmarks
        if host_performance:
            team_benchmarks = {
                "avg_score": round(sum(h["avg_score"] for h in host_performance) / len(host_performance), 1),
                "avg_discovery_depth": round(
                    sum(h["discovery_depth_score"] for h in host_performance) / len(host_performance), 1
                ),
                "avg_now_rate": round(sum(h["now_rate"] for h in host_performance) / len(host_performance), 1),
                "avg_measure_rate": round(
                    sum(h["measure_rate"] for h in host_performance) / len(host_performance), 1
                ),
                "avg_blocker_rate": round(
                    sum(h["blocker_rate"] for h in host_performance) / len(host_performance), 1
                ),
            }
        else:
            team_benchmarks = {}

        return {
            "host_performance": host_performance,
            "team_benchmarks": team_benchmarks,
            "trends": analytics.get_week_over_week_trends(),
            "summary": self._get_summary_metrics(days),
            "top_performers": host_performance[:3] if host_performance else [],
            "improvement_areas": self._identify_improvement_areas(host_performance),
        }

    def _identify_improvement_areas(self, host_performance: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Identify team-wide improvement areas based on discovery metrics."""
        if not host_performance:
            return {}

        # Calculate average rates
        avg_now = sum(h["now_rate"] for h in host_performance) / len(host_performance)
        avg_measure = sum(h["measure_rate"] for h in host_performance) / len(host_performance)
        avg_blocker = sum(h["blocker_rate"] for h in host_performance) / len(host_performance)

        improvements = []

        if avg_now < 30:
            improvements.append({
                "area": "Urgency Discovery",
                "current": f"{avg_now:.1f}%",
                "target": "40%+",
                "suggestion": "Ask: 'What needs to happen in the next 60 days?'",
            })

        if avg_measure < 40:
            improvements.append({
                "area": "Metrics Discovery",
                "current": f"{avg_measure:.1f}%",
                "target": "50%+",
                "suggestion": "Ask: 'How will you measure success?'",
            })

        if avg_blocker < 35:
            improvements.append({
                "area": "Blocker Identification",
                "current": f"{avg_blocker:.1f}%",
                "target": "45%+",
                "suggestion": "Ask: 'What's preventing you from moving forward today?'",
            })

        return {"areas": improvements}

    def fetch_coaching_data(self, days: int = 7) -> Dict[str, Any]:
        """
        Fetch aggregated data for coaching mode.

        Returns:
        - summary_metrics: count, avg_score, pct_qualified
        - leaderboard: by desk
        - low_signal_analysis: criteria prevalence
        - sample_meetings: top 10 for context
        """
        start_date, end_date = self._get_date_window(days)

        # Summary metrics
        summary_query = f"""
        SELECT
            COUNT(*) as total_meetings,
            COUNTIF(qualified = TRUE) as qualified_meetings,
            AVG(total_qualified_sections) as avg_score,
            ROUND(100.0 * COUNTIF(qualified = TRUE) / COUNT(*), 1) as pct_qualified
        FROM `{self.table_id}`
        WHERE scored_at BETWEEN @start_date AND @end_date
        """

        # Leaderboard by desk
        leaderboard_query = f"""
        SELECT
            desk,
            COUNT(*) as num_calls,
            COUNTIF(qualified = TRUE) as qualified_calls,
            ROUND(AVG(total_qualified_sections), 1) as avg_score,
            ROUND(100.0 * COUNTIF(qualified = TRUE) / COUNT(*), 1) as pct_qualified
        FROM `{self.table_id}`
        WHERE scored_at BETWEEN @start_date AND @end_date
        GROUP BY desk
        ORDER BY avg_score DESC
        LIMIT 5
        """

        # Low-signal analysis (criteria prevalence)
        signal_query = f"""
        SELECT
            ROUND(100.0 * COUNTIF(JSON_VALUE(now, '$.qualified') = 'true') / COUNT(*), 1) as pct_now,
            ROUND(100.0 * COUNTIF(JSON_VALUE(next, '$.qualified') = 'true') / COUNT(*), 1) as pct_next,
            ROUND(100.0 * COUNTIF(JSON_VALUE(measure, '$.qualified') = 'true') / COUNT(*), 1) as pct_measure,
            ROUND(100.0 * COUNTIF(JSON_VALUE(blocker, '$.qualified') = 'true') / COUNT(*), 1) as pct_blocker
        FROM `{self.table_id}`
        WHERE scored_at BETWEEN @start_date AND @end_date AND qualified = TRUE
        """

        # Sample meetings for context
        sample_query = f"""
        SELECT
            meeting_id,
            date,
            desk,
            title,
            total_qualified_sections,
            now,
            next,
            measure,
            blocker,
            fit
        FROM `{self.table_id}`
        WHERE scored_at BETWEEN @start_date AND @end_date AND qualified = TRUE
        ORDER BY total_qualified_sections DESC
        LIMIT 10
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date),
                bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date),
            ]
        )

        logger.info(
            f"Fetching coaching data: {start_date.isoformat()} to {end_date.isoformat()}"
        )

        try:
            # Execute queries
            summary_job = self.client.query(summary_query, job_config=job_config)
            summary_results = list(summary_job.result())
            summary = dict(summary_results[0].items()) if summary_results else {}

            leaderboard_job = self.client.query(leaderboard_query, job_config=job_config)
            leaderboard = [dict(row.items()) for row in leaderboard_job.result()]

            signal_job = self.client.query(signal_query, job_config=job_config)
            signal_results = list(signal_job.result())
            signal_analysis = dict(signal_results[0].items()) if signal_results else {}

            sample_job = self.client.query(sample_query, job_config=job_config)
            sample_meetings = []
            for row in sample_job.result():
                row_dict = dict(row.items())
                # Parse JSON fields
                import json
                for field in ["now", "next", "measure", "blocker", "fit"]:
                    if isinstance(row_dict.get(field), str):
                        try:
                            row_dict[field] = json.loads(row_dict[field])
                        except (json.JSONDecodeError, TypeError):
                            pass
                sample_meetings.append(row_dict)

            result = {
                "summary": summary,
                "leaderboard": leaderboard,
                "signal_analysis": signal_analysis,
                "sample_meetings": sample_meetings,
            }

            logger.info(f"Fetched coaching data: {summary.get('total_meetings', 0)} total meetings")
            return result

        except Exception as e:
            logger.error(f"BigQuery coaching query failed: {e}", exc_info=True)
            raise


# Lazy-loaded global instance
_bq_client = None


def get_bq_client() -> BigQueryClient:
    """Get or create BigQuery client instance."""
    global _bq_client
    if _bq_client is None:
        _bq_client = BigQueryClient()
    return _bq_client
