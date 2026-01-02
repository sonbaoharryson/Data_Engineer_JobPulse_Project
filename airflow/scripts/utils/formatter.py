from typing import Any, Dict


def job_to_embed(job: Dict) -> Any:
    """Create a Discord Embed for a job.

    Import `discord` lazily so DAG parsing in environments where `discord.py` isn't
    installed (webserver, scheduler) won't fail. A runtime error is raised if
    someone actually tries to format an embed without `discord` available.
    """
    try:
        import discord
    except ImportError as exc:
        raise RuntimeError(
            "discord package is required to format job embeds. "
            "Install 'discord.py' in the runtime environment where sending is executed."
        ) from exc

    embed = discord.Embed(
        title=job.get('title', ''),
        url=job.get('url', ''),
        color=discord.Color.blue()
    )

    if job.get('logo'):
        embed.set_thumbnail(url=job['logo'])

    if job.get('location'):
        embed.add_field(name='📍 Location', value=job['location'], inline=True)

    embed.add_field(
        name="💰 Salary",
        value=job.get("salary") or "Negotiable",
        inline=True
    )

    embed.set_footer(text="New Job Alert 🚀")

    return embed