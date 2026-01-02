from discord.ext import commands
import discord
import os
from dotenv import load_dotenv
load_dotenv()

TOKEN = os.getenv('DISCORD_TOKEN')

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix='!', intents=intents)

@bot.event
async def on_ready():
    print(f'Bot logged in as {bot.user}')


@bot.command()
async def start(ctx):
    await ctx.send("👋 Welcome! Upload your resume to get matched with jobs.")


@bot.command()
async def jobs(ctx):
    await ctx.send("🔍 Job matching coming soon!")


def main():
    bot.run(TOKEN)


if __name__ == "__main__":
    main()