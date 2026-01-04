import asyncio
import logging
from .formatter import job_to_embed
from .db_conn import DBConnection
from sqlalchemy import text
from typing import List, Any

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

def query_unposted_jobs(table_name: str):
    engine = DBConnection().engine
    query = text(f"""
        SELECT *
        FROM job_db_sm4x.staging.{table_name}
        WHERE posted_to_discord = FALSE
    """)
    with engine.connect() as conn:
        result = conn.execute(query)
        rows = result.fetchall()

    jobs = [dict(row) for row in rows]
    urls = [row["url"] for row in rows]

    return jobs, urls

def mark_jobs_as_posted(table_name: str, job_urls: list):
    if not job_urls:
        return

    engine = DBConnection().engine
    query = text(f"""
        UPDATE job_db_sm4x.staging.{table_name}
        SET posted_to_discord = TRUE
        WHERE url = ANY(:urls)
    """)

    with engine.begin() as conn:
        conn.execute(query, {"urls": job_urls})

async def send_jobs(jobs: List[dict], token: str, channel_id: int, throttle: float = 0.5) -> Any:
    """Send embeds to Discord. Import `discord` lazily so module import doesn't fail during DAG parsing."""
    try:
        import discord
    except ImportError:
        logger.exception("discord package is not installed — cannot send messages. Install 'discord.py' in the worker environment.")
        return

    intents = discord.Intents.none()
    client = discord.Client(intents=intents)

    try:
        await client.login(token)
        channel = await client.fetch_channel(channel_id)
    except Exception as e:
        logger.exception("Failed to connect or fetch channel: %s", e)
        try:
            await client.close()
        except Exception:
            pass
        return

    try:
        for job in jobs:
            try:
                embed = job_to_embed(job)
                await channel.send(embed=embed)
                if throttle and throttle > 0:
                    await asyncio.sleep(throttle)
            except Exception:
                logger.exception("Failed to send one job message")
    finally:
        try:
            await client.close()
        except Exception:
            logger.exception("Error closing Discord client")

def send_job_alerts(jobs, token: str, channel_id: int):
    if not jobs:
        logger.info("No jobs to send")
        return

    asyncio.run(send_jobs(jobs, token, channel_id))