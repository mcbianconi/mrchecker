import asyncio
import logging
import os
from datetime import datetime, timedelta

import aiohttp
from dotenv import load_dotenv
from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_client import AsyncWebClient

load_dotenv()

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger("mrchecker")

GITLAB_TOKEN = os.getenv("GITLAB_TOKEN")
SLACK_TOKEN = os.getenv("SLACK_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")
GITLAB_PROJECT_ID = os.getenv("GITLAB_PROJECT_ID")
CHECK_INTERVAL = timedelta(minutes=int(os.getenv("CHECK_INTERVAL_MINUTES", "10")))
TIME_GOAL = timedelta(hours=int(os.getenv("TIME_GOAL_HOURS", "24")))


async def fetch_merge_requests(http_client: AsyncWebClient):
    endpoint = f"/api/v4/projects/{GITLAB_PROJECT_ID}/merge_requests"
    params = {
        "state": "merged",
        "order_by": "updated_at",
        "target_branch": "dev",
    }
    async with http_client.get(endpoint, params=params) as response:
        response.raise_for_status()
        return await response.json()


async def notify_slack(slack_client: AsyncWebClient, mr, time_open):
    message = f"MR <{mr['web_url']}|#{mr['iid']} {mr['title']}> foi aceito.\nTempo aberto: {get_display_hours(time_open)}\n(Meta: {get_display_hours(TIME_GOAL)})."
    response = await slack_client.chat_postMessage(
        channel=SLACK_CHANNEL_ID, text=message
    )
    logger.info("notificação enviada para o slack", extra=response["data"])


def get_display_hours(delta: timedelta) -> str:
    return f"{delta.total_seconds() / 60 / 60:.2f}h"


async def check_mrs():
    slack_client = AsyncWebClient(token=SLACK_TOKEN, logger=logger)

    http_client = aiohttp.ClientSession(
        base_url="https://gitlab.buser.com.br",
        headers={"Authorization": f"Bearer {GITLAB_TOKEN}"},
    )
    try:
        while True:
            mrs = await fetch_merge_requests(http_client)
            for mr in mrs:
                created_at = datetime.fromisoformat(mr["created_at"])
                merged_at = datetime.fromisoformat(mr["merged_at"])
                time_open = merged_at - created_at
                if time_open > TIME_GOAL:
                    await notify_slack(slack_client, mr, time_open)
            await asyncio.sleep(CHECK_INTERVAL.seconds)
    except SlackApiError as e:
        logger.exception(
            f"erro ao enviar notificação para o slack: {e.response['error']}"
        )
    finally:
        await http_client.close()


if __name__ == "__main__":
    if not all((GITLAB_TOKEN, SLACK_TOKEN, SLACK_CHANNEL_ID, GITLAB_PROJECT_ID)):
        raise RuntimeError(
            "GITLAB_TOKEN, SLACK_TOKEN, SLACK_CHANNEL_ID e GITLAB_PROJECT_ID são obrigatórios."
        )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(check_mrs())
