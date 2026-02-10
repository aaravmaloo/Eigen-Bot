
import asyncio
import os
import discord
from discord.ext import commands
from queue import Queue
from io import BytesIO

import edge_tts

class Say(commands.Cog):
    """Simple edge-tts voice command with queue + auto leave."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.queue = Queue()

        self.playing = False
        self.leave_task: asyncio.Task | None = None

    async def schedule_leave(self, vc: discord.VoiceClient):
        """Leave VC after idle timeout."""
        if self.leave_task and not self.leave_task.done():
            self.leave_task.cancel()

        async def leave_later():
            try:
                await asyncio.sleep(float(os.environ["TTS_VC_LEAVE_TIMEOUT"]))

                if vc.is_connected() and not vc.is_playing():
                    await vc.disconnect()
            except asyncio.CancelledError:
                pass

        self.leave_task = asyncio.create_task(leave_later())

    async def edge_to_bytes(self, text: str) -> BytesIO:
        """Generate TTS audio to memory."""
        comm = edge_tts.Communicate(text=text, voice=os.environ["TTS_VOICE"])

        fp = BytesIO()
        async for chunk in comm.stream():
            if chunk["type"] == "audio":
                fp.write(chunk["data"]) # type: ignore
        fp.seek(0)
        return fp

    async def process_queue(self, vc: discord.VoiceClient) -> None:
        """Play queued messages sequentially."""
        if self.playing:
            return
        
        self.playing = True

        while not self.queue.empty():
            text = self.queue.get()

            if self.leave_task:
                self.leave_task.cancel()

            audio = await self.edge_to_bytes(text=text)

            source = discord.FFmpegPCMAudio(audio, pipe=True, options="-s mp3")
            vc.play(source=source)

            while vc.is_playing():
                await asyncio.sleep(0.5)

            self.queue.task_done()

        self.playing = False
        await self.schedule_leave(vc)

    @commands.hybrid_command(name="tts")
    async def tts(self, ctx: commands.Context, *, text: str):
        """Speak text in author's VC."""
        if ctx.guild is None:
            embed = discord.Embed(
                title="Server Only",
                description="This command can only be used in a server.",
                color=discord.Color.red()
            )
            return await ctx.send(embed=embed)
        
        author = ctx.author
        if not isinstance(author, discord.Member):
            embed = discord.Embed(
                title="Server Member Only",
                description="Could not resolve you as a server member.",
                color=discord.Color.red()
            )
            return await ctx.send(embed=embed)
        
        if not author.voice or not author.voice.channel:
            embed = discord.Embed(
                title="Join VC",
                description="You must be in a voice channel to use this feature!",
                color=discord.Color.red()
            )
            return await ctx.send(embed=embed)
        
        channel = author.voice.channel

        if ctx.voice_client is None:
            vc = await channel.connect()
        else:
            vc = ctx.voice_client
            if vc.channel != channel:
                await vc.disconnect(force=True)
                vc = await channel.connect()

        self.queue.put(f"{ctx.author.display_name} said: " + text)
        embed = discord.Embed(
            title="Yapping 🗣",
            description=f"Message: **{text}**",
            color=discord.Color.green()
        )
        embed.set_author(name=ctx.author.display_name)
        await ctx.send(embed=embed)

        await self.process_queue(vc) # type: ignore


async def setup(bot: commands.Bot):
    await bot.add_cog(Say(bot=bot))


