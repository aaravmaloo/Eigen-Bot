import asyncio
import os
import discord
from discord.ext import commands
from queue import Queue
from io import BytesIO
import edge_tts


class Say(commands.Cog):
    """Edge TTS with queue, cooldown, custom login name and auto leave."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.queue: Queue[str] = Queue()
        self.playing: bool = False
        self.leave_task: asyncio.Task | None = None
        self.tts_names: dict[int, str] = {}

    # ----------------------------
    # AUTO LEAVE
    # ----------------------------

    async def schedule_leave(self, vc: discord.VoiceClient) -> None:
        if self.leave_task and not self.leave_task.done():
            self.leave_task.cancel()

        async def leave_later():
            try:
                timeout = float(os.getenv("TTS_VC_LEAVE_TIMEOUT", "30"))
                await asyncio.sleep(timeout)

                if vc.is_connected() and not vc.is_playing():
                    await vc.disconnect()
            except asyncio.CancelledError:
                pass

        self.leave_task = asyncio.create_task(leave_later())

    # ----------------------------
    # GENERATE AUDIO
    # ----------------------------

    async def edge_to_bytes(self, text: str) -> BytesIO:
        voice = os.getenv("TTS_VOICE", "en-US-AriaNeural")
        comm = edge_tts.Communicate(text=text, voice=voice)

        fp = BytesIO()
        async for chunk in comm.stream():
            if chunk["type"] == "audio":
                fp.write(chunk["data"])

        fp.seek(0)
        return fp

    # ----------------------------
    # QUEUE PROCESSOR
    # ----------------------------

    async def process_queue(self, vc: discord.VoiceClient) -> None:
        if self.playing:
            return

        self.playing = True

        while not self.queue.empty():
            text = self.queue.get()

            if self.leave_task:
                self.leave_task.cancel()

            audio = await self.edge_to_bytes(text)
            source = discord.FFmpegPCMAudio(audio, pipe=True)

            vc.play(source)

            while vc.is_playing():
                await asyncio.sleep(0.5)

            self.queue.task_done()

        self.playing = False
        await self.schedule_leave(vc)

    # ----------------------------
    # LOGIN TTS NAME
    # ----------------------------

    @commands.hybrid_command(name="logintts")
    async def logintts(self, ctx: commands.Context, name: str):
        if len(name) > 32:
            return await ctx.send("Name too long. Max 32 characters.")

        self.tts_names[ctx.author.id] = name.strip()
        await ctx.send(f"TTS name set to: {name}")

    # ----------------------------
    # FORCE LEAVE VC
    # ----------------------------

    @commands.hybrid_command(name="leavevc")
    async def leavevc(self, ctx: commands.Context):
        vc = ctx.voice_client
        if isinstance(vc, discord.VoiceClient) and vc.is_connected():
            await vc.disconnect(force=True)
            await ctx.send("Left the voice channel.")
        else:
            await ctx.send("I am not in a voice channel.")

    # ----------------------------
    # TTS COMMAND
    # ----------------------------

    @commands.hybrid_command(name="tts")
    @commands.cooldown(1, 2, commands.BucketType.user)
    async def tts(self, ctx: commands.Context, *, text: str):

        if ctx.guild is None:
            return await ctx.send("Server only command.")

        if len(text) > 400:
            return await ctx.send("Maximum 400 characters allowed.")

        author = ctx.author
        if not isinstance(author, discord.Member):
            return await ctx.send("Server member only.")

        if not author.voice or not author.voice.channel:
            return await ctx.send("Join a voice channel first.")

        channel = author.voice.channel

        vc = ctx.voice_client

        if not isinstance(vc, discord.VoiceClient):
            vc = await channel.connect()
        elif vc.channel != channel:
            await vc.disconnect(force=True)
            vc = await channel.connect()

        content = text

        # Replace mentions
        if ctx.message:
            for member in ctx.message.mentions:
                content = content.replace(
                    f"<@{member.id}>", f"@{member.display_name}"
                )
                content = content.replace(
                    f"<@!{member.id}>", f"@{member.display_name}"
                )

            for channel_ in ctx.message.channel_mentions:
                content = content.replace(
                    f"<#{channel_.id}>", f"#{channel_.name}"
                )

        tts_name = self.tts_names.get(ctx.author.id, ctx.author.display_name)

        self.queue.put(f"{tts_name} said {content}")

        await ctx.send(f"Queued: {text}")

        await self.process_queue(vc)

    # ----------------------------
    # COOLDOWN ERROR HANDLER
    # ----------------------------

    @tts.error
    async def tts_error(self, ctx: commands.Context, error):
        if isinstance(error, commands.CommandOnCooldown):
            await ctx.send("You are sending TTS too fast. Wait 2 seconds.")
        else:
            raise error


async def setup(bot: commands.Bot):
    await bot.add_cog(Say(bot))
