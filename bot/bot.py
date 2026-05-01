import os
import discord
from discord.ext import commands
from dotenv import load_dotenv
from functions.reponse import generate_response
import logging
import sys

# Configure logging to output to stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")

intents = discord.Intents.default()
intents.message_content = True


class MyBot(commands.Bot):
    async def setup_hook(self):
        # Sync slash commands to Discord
        await self.tree.sync()
        logger.info("Slash commands synced!")


bot = MyBot(command_prefix="!", intents=intents)


@bot.event
async def on_ready():
    logger.info(f"Bot logged in as {bot.user}")


@bot.tree.command(name="chat", description="Chat with Job Pulse Consultant")
async def chat(interaction: discord.Interaction):
    await interaction.user.send(
        f"Hello {interaction.user.name}! 😊 What can I do for you today?"
    )
    await interaction.response.send_message("👋 I've sent you a DM!", ephemeral=True)


@bot.event
async def on_message(message):
    if message.author == bot.user:
        return

    if isinstance(message.channel, discord.DMChannel):
        user_message = message.content
        user_id = str(message.author)  # .id)
        print(f"Received DM from {message.author}: {user_message}")

        response = await generate_response(user_id, user_message)

        print(f"Generated response: {response}")

        if len(response) > 2000:
            chunks = [response[i : i + 2000] for i in range(0, len(response), 2000)]
            for chunk in chunks:
                await message.author.send(chunk)
        else:
            await message.author.send(response)

        # await message.channel.send(response)


def main():
    bot.run(TOKEN)


if __name__ == "__main__":
    main()
