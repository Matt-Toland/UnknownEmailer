"""Zapier webhook sender for email delivery."""
import logging
from typing import Dict

import httpx

from app.config import config

logger = logging.getLogger(__name__)


class EmailSender:
    """Zapier webhook email sender."""

    def __init__(self):
        self.hook_url = config.ZAPIER_HOOK_URL
        self.timeout = 30.0

    async def send_email(
        self,
        to: str,
        subject: str,
        html: str,
    ) -> Dict[str, str]:
        """
        Send email via Zapier webhook.

        Args:
            to: Recipient email address
            subject: Email subject line
            html: Full HTML email content

        Returns:
            Dict with status and optional message

        Raises:
            httpx.HTTPError: If webhook request fails
        """
        payload = {
            "to": to,
            "subject": subject,
            "html": html,
        }

        logger.info(f"Sending email to {to} via Zapier webhook")

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    self.hook_url,
                    json=payload,
                )
                response.raise_for_status()

                logger.info(f"Email sent successfully to {to}: {response.status_code}")
                return {
                    "status": "sent",
                    "message": f"Email sent to {to}",
                }

        except httpx.HTTPStatusError as e:
            logger.error(
                f"Zapier webhook failed: {e.response.status_code} - {e.response.text}",
                exc_info=True,
            )
            raise

        except httpx.RequestError as e:
            logger.error(f"Zapier webhook request error: {e}", exc_info=True)
            raise

        except Exception as e:
            logger.error(f"Unexpected error sending email: {e}", exc_info=True)
            raise


# Lazy-loaded global instance
_email_sender = None


def get_email_sender() -> EmailSender:
    """Get or create email sender instance."""
    global _email_sender
    if _email_sender is None:
        _email_sender = EmailSender()
    return _email_sender
