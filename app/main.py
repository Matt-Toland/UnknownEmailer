"""FastAPI application for UNKNOWN Brain weekly email service."""
import logging
import uuid
from contextlib import asynccontextmanager
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, EmailStr

from app.bq import get_bq_client
from app.config import config
from app.llm import get_llm_client
from app.render import get_email_subject, render_email
from app.sender import get_email_sender

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown."""
    # Startup
    logger.info("Starting UNKNOWN Brain email service")
    try:
        config.validate()
        logger.info("Configuration validated successfully")
    except ValueError as e:
        logger.error(f"Configuration validation failed: {e}")
        raise
    yield
    # Shutdown
    logger.info("Shutting down UNKNOWN Brain email service")


app = FastAPI(
    title="UNKNOWN Brain Email Service",
    description="Weekly meeting intelligence email generator",
    version="1.0.0",
    lifespan=lifespan,
)


class SendEmailRequest(BaseModel):
    """Request body for sending email."""

    mode: Literal["insights", "coaching"]
    to: Optional[str] = None  # Comma-separated email addresses


@app.middleware("http")
async def add_request_id(request: Request, call_next):
    """Add request_id to all requests for logging."""
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id

    # Add to logging context
    logger.info(f"[{request_id}] {request.method} {request.url.path}")

    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "service": "UNKNOWN Brain Email Service",
        "status": "healthy",
        "version": "1.0.0",
    }


@app.get("/debug/data")
async def debug_data(request: Request = None):
    """Debug endpoint to see raw data structure."""
    request_id = getattr(request.state, "request_id", "unknown")
    logger.info(f"[{request_id}] Debug data fetch")

    try:
        bq_client = get_bq_client()
        intelligence_data = bq_client.fetch_insights_data_v2(days=40)

        # Only return first 2 items from now_pipeline for inspection
        if intelligence_data.get("now_pipeline"):
            intelligence_data["now_pipeline"] = intelligence_data["now_pipeline"][:2]

        # Simplify for readability
        debug_output = {
            "now_pipeline_sample": intelligence_data.get("now_pipeline", []),
            "client_concentration_count": len(intelligence_data.get("client_concentration", {}).get("top_clients", [])),
            "summary": intelligence_data.get("summary_metrics", {})
        }

        return JSONResponse(content=debug_output)

    except Exception as e:
        logger.error(f"[{request_id}] Debug failed: {e}", exc_info=True)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/health")
async def health():
    """Kubernetes health check endpoint."""
    return {"status": "ok"}


@app.get("/email/preview", response_class=HTMLResponse)
async def preview_email(
    mode: Literal["insights", "coaching"] = Query(..., description="Email mode"),
    days: Optional[int] = Query(None, description="Number of days to look back"),
    request: Request = None,
):
    """
    Preview email without sending.

    Returns rendered HTML email for the specified mode.
    """
    request_id = getattr(request.state, "request_id", "unknown")
    logger.info(f"[{request_id}] Generating preview for mode: {mode}")

    try:
        # Get client instances
        bq_client = get_bq_client()
        llm_client = get_llm_client()

        # Set the time period (default to 7 days for weekly reports)
        if days is None:
            days = 7  # Weekly reports

        # Fetch data from BigQuery
        if mode == "insights":
            logger.info(f"[{request_id}] Fetching v2 insights data from BigQuery for last {days} days")
            intelligence_data = bq_client.fetch_insights_data_v2(days=days)

            if not intelligence_data.get("summary_metrics"):
                return HTMLResponse(
                    content="<html><body><h1>No qualified meetings found in the specified period</h1></body></html>",
                    status_code=200,
                )

            # Generate content with LLM
            logger.info(f"[{request_id}] Generating insights content with LLM")
            markdown_content = llm_client.generate_insights_v2(intelligence_data)

        else:  # coaching
            logger.info(f"[{request_id}] Fetching coaching data from BigQuery")
            coaching_data = bq_client.fetch_coaching_data()

            if not coaching_data.get("summary"):
                return HTMLResponse(
                    content="<html><body><h1>No meeting data found in the last 7 days</h1></body></html>",
                    status_code=200,
                )

            # Generate content with LLM
            logger.info(f"[{request_id}] Generating coaching content with LLM")
            markdown_content = llm_client.generate_coaching(coaching_data)

        # Render email HTML
        logger.info(f"[{request_id}] Rendering email HTML")
        total_meetings = None
        if mode == "insights":
            total_meetings = intelligence_data.get("summary_metrics", {}).get("total_meetings")
        else:
            total_meetings = coaching_data.get("summary", {}).get("total_meetings")

        html = render_email(mode, markdown_content, total_meetings, days=days)

        logger.info(f"[{request_id}] Preview generated successfully")
        return HTMLResponse(content=html)

    except Exception as e:
        logger.error(f"[{request_id}] Preview generation failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate preview: {str(e)}",
        )


@app.get("/email/preview/v2", response_class=HTMLResponse)
async def preview_email_v2(
    mode: Literal["insights", "coaching"] = Query(..., description="Email mode"),
    days: Optional[int] = Query(None, description="Number of days to analyze (default: 7 for production, 40 for testing)"),
    request: Request = None,
):
    """
    Preview v2 email with enhanced business intelligence.

    Returns rendered HTML email for the specified mode using new data structure.
    """
    request_id = getattr(request.state, "request_id", "unknown")
    logger.info(f"[{request_id}] Generating v2 preview for mode: {mode}")

    try:
        # Get client instances
        bq_client = get_bq_client()
        llm_client = get_llm_client()

        # Set the time period (default to 7 days for weekly reports)
        if days is None:
            days = 7  # Weekly reports

        # Fetch data from BigQuery
        if mode == "insights":
            logger.info(f"[{request_id}] Fetching v2 insights data from BigQuery for last {days} days")
            intelligence_data = bq_client.fetch_insights_data_v2(days=days)

            if not intelligence_data.get("summary_metrics"):
                return HTMLResponse(
                    content="<html><body><h1>No data found in the specified period</h1></body></html>",
                    status_code=200,
                )

            # Generate content with LLM
            logger.info(f"[{request_id}] Generating v2 insights content with LLM")
            markdown_content = llm_client.generate_insights_v2(intelligence_data)

        else:  # coaching
            logger.info(f"[{request_id}] Fetching v2 coaching data from BigQuery for last {days} days")
            coaching_data = bq_client.fetch_coaching_data_v2(days=days)

            if not coaching_data.get("summary"):
                return HTMLResponse(
                    content="<html><body><h1>No meeting data found in the specified period</h1></body></html>",
                    status_code=200,
                )

            # Generate content with LLM
            logger.info(f"[{request_id}] Generating v2 coaching content with LLM")
            markdown_content = llm_client.generate_coaching_v2(coaching_data)

        # Render email HTML
        logger.info(f"[{request_id}] Rendering email HTML")
        total_meetings = None
        if mode == "insights":
            total_meetings = intelligence_data.get("summary_metrics", {}).get("total_meetings")
        else:
            total_meetings = coaching_data.get("summary", {}).get("total_meetings")

        html = render_email(mode, markdown_content, total_meetings, days=days)

        logger.info(f"[{request_id}] V2 preview generated successfully")
        return HTMLResponse(content=html)

    except Exception as e:
        logger.error(f"[{request_id}] V2 preview generation failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate v2 preview: {str(e)}",
        )


@app.post("/email/send")
async def send_email(
    body: SendEmailRequest,
    request: Request = None,
):
    """
    Generate and send email via Zapier webhook.

    Fetches data from BigQuery, generates content with LLM,
    renders HTML, and sends via Zapier.
    """
    request_id = getattr(request.state, "request_id", "unknown")
    logger.info(f"[{request_id}] Sending email: mode={body.mode}, to={body.to}")

    # Determine recipient
    recipient = body.to or config.INSIGHTS_SEND_TO
    if not recipient:
        raise HTTPException(
            status_code=400,
            detail="No recipient specified and INSIGHTS_SEND_TO not configured",
        )

    try:
        # Get client instances
        bq_client = get_bq_client()
        llm_client = get_llm_client()
        email_sender = get_email_sender()

        # Set time period - 7 days for weekly reports
        days = 7

        # Fetch data from BigQuery using v2 functions
        if body.mode == "insights":
            logger.info(f"[{request_id}] Fetching insights data from BigQuery for last {days} days")
            intelligence_data = bq_client.fetch_insights_data_v2(days=days)

            if not intelligence_data.get("summary_metrics"):
                raise HTTPException(
                    status_code=404,
                    detail=f"No qualified meetings found in the last {days} days",
                )

            # Generate content with LLM
            logger.info(f"[{request_id}] Generating insights content with LLM using v2")
            markdown_content = llm_client.generate_insights_v2(intelligence_data)
            total_meetings = intelligence_data.get("summary_metrics", {}).get("total_meetings")

        else:  # coaching
            logger.info(f"[{request_id}] Fetching coaching data from BigQuery for last {days} days")
            coaching_data = bq_client.fetch_coaching_data_v2(days=days)

            if not coaching_data.get("summary"):
                raise HTTPException(
                    status_code=404,
                    detail=f"No meeting data found in the last {days} days",
                )

            # Generate content with LLM
            logger.info(f"[{request_id}] Generating coaching content with LLM using v2")
            markdown_content = llm_client.generate_coaching_v2(coaching_data)
            total_meetings = coaching_data.get("summary", {}).get("total_meetings")

        # Render email HTML
        logger.info(f"[{request_id}] Rendering email HTML")
        html = render_email(body.mode, markdown_content, total_meetings, days=days)

        # Generate subject
        subject = get_email_subject(body.mode)

        # Send via Zapier
        logger.info(f"[{request_id}] Sending email to {recipient}")
        result = await email_sender.send_email(
            to=recipient,
            subject=subject,
            html=html,
        )

        logger.info(f"[{request_id}] Email sent successfully")
        return JSONResponse(content=result)

    except HTTPException:
        raise

    except Exception as e:
        logger.error(f"[{request_id}] Email send failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=502,
            detail=f"Failed to send email: {str(e)}",
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
