import logging
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import aiosqlite
import discord
from discord import app_commands
from discord.ext import commands, tasks

from utils.codebuddy_database import DB_PATH

DB_TIMEOUT = 30.0
logger = logging.getLogger(__name__)


def _connect_db():
    return aiosqlite.connect(DB_PATH, timeout=DB_TIMEOUT)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_datetime(value: str) -> Optional[datetime]:
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    if " " in raw and "T" not in raw:
        raw = raw.replace(" ", "T", 1)
    if len(raw) == 10:
        raw = raw + "T00:00:00+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_ts(dt: datetime) -> str:
    return f"<t:{int(dt.timestamp())}:F>"


def _format_rel(dt: datetime) -> str:
    return f"<t:{int(dt.timestamp())}:R>"


class SeasonalEvents(commands.Cog):
    """Seasonal events / live ops with leaderboards and daily check-ins."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def cog_load(self):
        if not self.event_tick.is_running():
            self.event_tick.start()

    def cog_unload(self):
        if self.event_tick.is_running():
            self.event_tick.cancel()

    async def _fetch_events(self, guild_id: Optional[int] = None) -> List[tuple]:
        query = (
            "SELECT event_id, guild_id, name, description, start_at, end_at, status, announcement_channel_id "
            "FROM events WHERE status IN ('scheduled', 'active')"
        )
        params = []
        if guild_id is not None:
            query += " AND guild_id = ?"
            params.append(guild_id)
        query += " ORDER BY guild_id ASC, start_at ASC"
        async with _connect_db() as db:
            cursor = await db.execute(query, tuple(params))
            return await cursor.fetchall()

    async def _set_status(self, event_id: int, status: str, *, start_at: Optional[datetime] = None, end_at: Optional[datetime] = None):
        fields = ["status = ?"]
        values = [status]
        if start_at is not None:
            fields.append("start_at = ?")
            values.append(start_at.isoformat())
        if end_at is not None:
            fields.append("end_at = ?")
            values.append(end_at.isoformat())
        values.append(event_id)
        async with _connect_db() as db:
            await db.execute(f"UPDATE events SET {', '.join(fields)} WHERE event_id = ?", values)
            await db.commit()

    async def _get_active_event(self, guild_id: int) -> Optional[tuple]:
        async with _connect_db() as db:
            cursor = await db.execute(
                "SELECT event_id, name, description, start_at, end_at, status, announcement_channel_id "
                "FROM events WHERE guild_id = ? AND status = 'active' "
                "ORDER BY start_at ASC LIMIT 1",
                (guild_id,)
            )
            row = await cursor.fetchone()
        return row

    async def _get_next_event(self, guild_id: int) -> Optional[tuple]:
        async with _connect_db() as db:
            cursor = await db.execute(
                "SELECT event_id, name, description, start_at, end_at, status, announcement_channel_id "
                "FROM events WHERE guild_id = ? AND status = 'scheduled' "
                "ORDER BY start_at ASC LIMIT 1",
                (guild_id,)
            )
            return await cursor.fetchone()

    async def _sync_events(self, rows: List[tuple]):
        now = _utcnow()
        for row in rows:
            event_id, guild_id, name, description, start_at, end_at, status, channel_id = row
            try:
                start_dt = datetime.fromisoformat(start_at)
                end_dt = datetime.fromisoformat(end_at)
            except ValueError:
                logger.warning(
                    "Skipping event %s due to invalid datetime values: start_at=%r end_at=%r",
                    event_id,
                    start_at,
                    end_at,
                )
                continue
            if status == "scheduled" and start_dt <= now:
                await self._set_status(event_id, "active")
                await self._announce_start(guild_id, event_id, name, description, start_dt, end_dt, channel_id)
            elif status == "active" and end_dt <= now:
                await self._set_status(event_id, "ended")
                await self._announce_end(guild_id, event_id, name, description, start_dt, end_dt, channel_id)

    async def _sync_guild(self, guild_id: int):
        rows = await self._fetch_events(guild_id)
        await self._sync_events(rows)

    async def _announce_start(self, guild_id: int, event_id: int, name: str, description: str,
                              start_dt: datetime, end_dt: datetime, channel_id: Optional[int]):
        guild = self.bot.get_guild(guild_id)
        if not guild or channel_id is None:
            return
        channel = guild.get_channel(channel_id)
        if not isinstance(channel, discord.TextChannel):
            return
        embed = discord.Embed(
            title=f"Event Started: {name}",
            description=description or "Jump in and start earning points.",
            color=0x2ECC71
        )
        embed.add_field(name="Ends", value=f"{_format_ts(end_dt)} ({_format_rel(end_dt)})", inline=False)
        embed.add_field(name="How to Join", value="Use `/event join` to enter.", inline=False)
        await channel.send(embed=embed)

    async def _announce_end(self, guild_id: int, event_id: int, name: str, description: str,
                            start_dt: datetime, end_dt: datetime, channel_id: Optional[int]):
        guild = self.bot.get_guild(guild_id)
        if not guild or channel_id is None:
            return
        channel = guild.get_channel(channel_id)
        if not isinstance(channel, discord.TextChannel):
            return
        winners = await self._top_participants(event_id, 3)
        lines = []
        for i, (user_id, points) in enumerate(winners, start=1):
            member = guild.get_member(user_id)
            mention = member.mention if member else f"<@{user_id}>"
            lines.append(f"{i}. {mention} — {points} pts")
        summary = "\n".join(lines) if lines else "No participants this time."
        embed = discord.Embed(
            title=f"Event Ended: {name}",
            description=description or "Thanks for playing.",
            color=0x95A5A6
        )
        embed.add_field(name="Winners", value=summary, inline=False)
        embed.add_field(name="Duration", value=f"{_format_ts(start_dt)} to {_format_ts(end_dt)}", inline=False)
        await channel.send(embed=embed)

    async def _top_participants(self, event_id: int, limit: int) -> List[Tuple[int, int]]:
        async with _connect_db() as db:
            cursor = await db.execute(
                "SELECT user_id, points FROM event_participants WHERE event_id = ? "
                "ORDER BY points DESC, last_activity DESC LIMIT ?",
                (event_id, limit)
            )
            return await cursor.fetchall()

    async def _ensure_joined(self, event_id: int, user_id: int) -> bool:
        async with _connect_db() as db:
            cursor = await db.execute(
                "SELECT 1 FROM event_participants WHERE event_id = ? AND user_id = ?",
                (event_id, user_id)
            )
            exists = await cursor.fetchone()
            if exists:
                return True
            now = _utcnow().isoformat()
            await db.execute(
                "INSERT INTO event_participants (event_id, user_id, points, joined_at, last_activity) "
                "VALUES (?, ?, 0, ?, ?)",
                (event_id, user_id, now, now)
            )
            await db.commit()
            return False

    async def _log_point_action(
        self,
        db: aiosqlite.Connection,
        event_id: int,
        user_id: int,
        points: int,
        reason: str,
        *,
        created_at: str,
    ):
        await db.execute(
            "INSERT INTO event_point_actions (event_id, user_id, points, reason, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (event_id, user_id, points, reason, created_at),
        )

    async def _add_points(self, event_id: int, user_id: int, points: int, reason: str):
        now = _utcnow().isoformat()
        async with _connect_db() as db:
            await db.execute(
                "UPDATE event_participants SET points = points + ?, last_activity = ? "
                "WHERE event_id = ? AND user_id = ?",
                (points, now, event_id, user_id)
            )
            await self._log_point_action(db, event_id, user_id, points, reason, created_at=now)
            await db.commit()

    @tasks.loop(minutes=1)
    async def event_tick(self):
        try:
            rows = await self._fetch_events()
        except Exception:
            logger.exception("Failed to fetch event rows during event tick")
            return
        if not rows:
            return

        events_by_guild = {}
        for row in rows:
            events_by_guild.setdefault(row[1], []).append(row)

        for guild_id in sorted(events_by_guild):
            try:
                await self._sync_events(events_by_guild[guild_id])
            except Exception:
                logger.exception("Failed to sync events for guild %s", guild_id)

    @event_tick.before_loop
    async def before_tick(self):
        await self.bot.wait_until_ready()

    @commands.hybrid_group(name="event", description="Seasonal events and live ops.")
    @commands.guild_only()
    async def event_group(self, ctx: commands.Context):
        if ctx.invoked_subcommand is None:
            await self.event_status(ctx)

    @event_group.command(name="status", description="Show the current or upcoming event.")
    @commands.guild_only()
    async def event_status(self, ctx: commands.Context):
        if ctx.guild is None:
            return await ctx.reply("This command can only be used in a server.")
        await self._sync_guild(ctx.guild.id)
        active = await self._get_active_event(ctx.guild.id)
        if active:
            event_id, name, description, start_at, end_at, status, channel_id = active
            start_dt = datetime.fromisoformat(start_at)
            end_dt = datetime.fromisoformat(end_at)
            embed = discord.Embed(
                title=f"Active Event: {name}",
                description=description or "Live now.",
                color=0x3498DB
            )
            embed.add_field(name="Started", value=f"{_format_ts(start_dt)} ({_format_rel(start_dt)})", inline=False)
            embed.add_field(name="Ends", value=f"{_format_ts(end_dt)} ({_format_rel(end_dt)})", inline=False)
            embed.add_field(name="Join", value="Use `/event join` to enter.", inline=False)
            return await ctx.reply(embed=embed)

        upcoming = await self._get_next_event(ctx.guild.id)
        if upcoming:
            event_id, name, description, start_at, end_at, status, channel_id = upcoming
            start_dt = datetime.fromisoformat(start_at)
            end_dt = datetime.fromisoformat(end_at)
            embed = discord.Embed(
                title=f"Upcoming Event: {name}",
                description=description or "Get ready.",
                color=0xF1C40F
            )
            embed.add_field(name="Starts", value=f"{_format_ts(start_dt)} ({_format_rel(start_dt)})", inline=False)
            embed.add_field(name="Ends", value=_format_ts(end_dt), inline=False)
            return await ctx.reply(embed=embed)

        await ctx.reply("No events scheduled right now.")

    @event_group.command(name="create", description="Create a seasonal event.")
    @app_commands.describe(
        name="Event name",
        start="Start time (YYYY-MM-DD HH:MM or ISO 8601, UTC if no timezone)",
        end="End time (YYYY-MM-DD HH:MM or ISO 8601, UTC if no timezone)",
        channel="Announcement channel",
        description="Optional event description"
    )
    @commands.has_permissions(manage_events=True)
    @commands.guild_only()
    async def event_create(
        self,
        ctx: commands.Context,
        name: str,
        start: str,
        end: str,
        channel: Optional[discord.TextChannel] = None,
        *,
        description: str = ""
    ):
        if ctx.guild is None:
            return await ctx.reply("This command can only be used in a server.")
        start_dt = _parse_datetime(start)
        end_dt = _parse_datetime(end)
        if not start_dt or not end_dt:
            return await ctx.reply("Invalid date format. Use `YYYY-MM-DD HH:MM` or ISO 8601.")
        if end_dt <= start_dt:
            return await ctx.reply("End time must be after the start time.")

        async with _connect_db() as db:
            cursor = await db.execute(
                "SELECT event_id, name, start_at, end_at FROM events WHERE guild_id = ? AND status IN ('scheduled', 'active')",
                (ctx.guild.id,)
            )
            existing = await cursor.fetchall()
            for row in existing:
                ex_start = datetime.fromisoformat(row[2])
                ex_end = datetime.fromisoformat(row[3])
                if not (end_dt <= ex_start or start_dt >= ex_end):
                    return await ctx.reply(f"Another event overlaps with that time window: `{row[1]}`.")

            await db.execute(
                "INSERT INTO events (guild_id, name, description, start_at, end_at, status, announcement_channel_id, created_by, created_at) "
                "VALUES (?, ?, ?, ?, ?, 'scheduled', ?, ?, ?)",
                (
                    ctx.guild.id,
                    name,
                    description,
                    start_dt.isoformat(),
                    end_dt.isoformat(),
                    channel.id if channel else None,
                    ctx.author.id,
                    _utcnow().isoformat()
                )
            )
            await db.commit()

        embed = discord.Embed(
            title="Event Scheduled",
            description=description or name,
            color=0x2ECC71
        )
        embed.add_field(name="Starts", value=f"{_format_ts(start_dt)} ({_format_rel(start_dt)})", inline=False)
        embed.add_field(name="Ends", value=_format_ts(end_dt), inline=False)
        if channel:
            embed.add_field(name="Announcements", value=channel.mention, inline=False)
        await ctx.reply(embed=embed)

    @event_group.command(name="join", description="Join the active event.")
    @commands.guild_only()
    async def event_join(self, ctx: commands.Context):
        if ctx.guild is None:
            return await ctx.reply("This command can only be used in a server.")
        await self._sync_guild(ctx.guild.id)
        active = await self._get_active_event(ctx.guild.id)
        if not active:
            return await ctx.reply("No active event right now.")
        event_id, name, description, start_at, end_at, status, channel_id = active
        already = await self._ensure_joined(event_id, ctx.author.id)
        if already:
            return await ctx.reply("You are already in this event.")
        await ctx.reply(f"Joined **{name}**. Use `/event checkin` to earn points.")

    @event_group.command(name="checkin", description="Daily check-in for the active event.")
    @commands.guild_only()
    async def event_checkin(self, ctx: commands.Context):
        if ctx.guild is None:
            return await ctx.reply("This command can only be used in a server.")
        await self._sync_guild(ctx.guild.id)
        active = await self._get_active_event(ctx.guild.id)
        if not active:
            return await ctx.reply("No active event right now.")
        event_id, name, description, start_at, end_at, status, channel_id = active
        await self._ensure_joined(event_id, ctx.author.id)

        now_dt = _utcnow()
        today = now_dt.date().isoformat()
        now = now_dt.isoformat()
        async with _connect_db() as db:
            cursor = await db.execute(
                "SELECT last_checkin FROM event_participants WHERE event_id = ? AND user_id = ?",
                (event_id, ctx.author.id)
            )
            row = await cursor.fetchone()
            if row and row[0] == today:
                return await ctx.reply("You already checked in today. Try again tomorrow.")
            await db.execute(
                "UPDATE event_participants SET last_checkin = ?, last_activity = ?, points = points + 1 "
                "WHERE event_id = ? AND user_id = ?",
                (today, now, event_id, ctx.author.id)
            )
            await self._log_point_action(db, event_id, ctx.author.id, 1, "daily_checkin", created_at=now)
            await db.commit()

        await ctx.reply("Check-in recorded. +1 point.")

    @event_group.command(name="leaderboard", description="Show top event players.")
    @commands.guild_only()
    async def event_leaderboard(self, ctx: commands.Context):
        if ctx.guild is None:
            return await ctx.reply("This command can only be used in a server.")
        await self._sync_guild(ctx.guild.id)
        active = await self._get_active_event(ctx.guild.id)
        if not active:
            return await ctx.reply("No active event right now.")
        event_id, name, description, start_at, end_at, status, channel_id = active
        top = await self._top_participants(event_id, 10)
        if not top:
            return await ctx.reply("No participants yet.")
        lines = []
        for i, (user_id, points) in enumerate(top, start=1):
            member = ctx.guild.get_member(user_id)
            mention = member.mention if member else f"<@{user_id}>"
            lines.append(f"{i}. {mention} — {points} pts")
        embed = discord.Embed(
            title=f"{name} Leaderboard",
            description="\n".join(lines),
            color=0x9B59B6
        )
        await ctx.reply(embed=embed)

    @event_group.command(name="me", description="Show your event stats.")
    @commands.guild_only()
    async def event_me(self, ctx: commands.Context):
        if ctx.guild is None:
            return await ctx.reply("This command can only be used in a server.")
        await self._sync_guild(ctx.guild.id)
        active = await self._get_active_event(ctx.guild.id)
        if not active:
            return await ctx.reply("No active event right now.")
        event_id, name, description, start_at, end_at, status, channel_id = active
        async with _connect_db() as db:
            cursor = await db.execute(
                "SELECT points, last_checkin FROM event_participants WHERE event_id = ? AND user_id = ?",
                (event_id, ctx.author.id)
            )
            row = await cursor.fetchone()
        if not row:
            return await ctx.reply("You are not in the event yet. Use `/event join`.")
        points, last_checkin = row
        embed = discord.Embed(
            title=f"{name} — Your Stats",
            description=f"Points: **{points}**",
            color=0x1ABC9C
        )
        if last_checkin:
            embed.add_field(name="Last Check-in", value=last_checkin, inline=False)
        await ctx.reply(embed=embed)

    @event_group.command(name="award", description="Award points to a participant.")
    @app_commands.describe(user="User to award", points="Points to add", reason="Reason for the award")
    @commands.has_permissions(manage_events=True)
    @commands.guild_only()
    async def event_award(self, ctx: commands.Context, user: discord.Member, points: int, *, reason: str = "manual_award"):
        if ctx.guild is None:
            return await ctx.reply("This command can only be used in a server.")
        await self._sync_guild(ctx.guild.id)
        active = await self._get_active_event(ctx.guild.id)
        if not active:
            return await ctx.reply("No active event right now.")
        if points == 0:
            return await ctx.reply("Points must be non-zero.")
        event_id, name, description, start_at, end_at, status, channel_id = active
        await self._ensure_joined(event_id, user.id)
        await self._add_points(event_id, user.id, points, reason)
        await ctx.reply(f"Awarded {points} points to {user.mention}.")

    @event_group.command(name="start", description="Manually start the next scheduled event.")
    @commands.has_permissions(manage_events=True)
    @commands.guild_only()
    async def event_start(self, ctx: commands.Context):
        if ctx.guild is None:
            return await ctx.reply("This command can only be used in a server.")
        active = await self._get_active_event(ctx.guild.id)
        if active:
            return await ctx.reply("There is already an active event.")
        upcoming = await self._get_next_event(ctx.guild.id)
        if not upcoming:
            return await ctx.reply("No scheduled events to start.")
        event_id, name, description, start_at, end_at, status, channel_id = upcoming
        now = _utcnow()
        await self._set_status(event_id, "active", start_at=now)
        await self._announce_start(ctx.guild.id, event_id, name, description, now, datetime.fromisoformat(end_at), channel_id)
        await ctx.reply(f"Started **{name}**.")

    @event_group.command(name="end", description="Manually end the active event.")
    @commands.has_permissions(manage_events=True)
    @commands.guild_only()
    async def event_end(self, ctx: commands.Context):
        if ctx.guild is None:
            return await ctx.reply("This command can only be used in a server.")
        active = await self._get_active_event(ctx.guild.id)
        if not active:
            return await ctx.reply("No active event to end.")
        event_id, name, description, start_at, end_at, status, channel_id = active
        now = _utcnow()
        await self._set_status(event_id, "ended", end_at=now)
        await self._announce_end(ctx.guild.id, event_id, name, description, datetime.fromisoformat(start_at), now, channel_id)
        await ctx.reply(f"Ended **{name}**.")

    @event_group.command(name="cancel", description="Cancel a scheduled event.")
    @app_commands.describe(event_id="Event ID to cancel")
    @commands.has_permissions(manage_events=True)
    @commands.guild_only()
    async def event_cancel(self, ctx: commands.Context, event_id: int):
        if ctx.guild is None:
            return await ctx.reply("This command can only be used in a server.")
        async with _connect_db() as db:
            cursor = await db.execute(
                "SELECT name, status FROM events WHERE event_id = ? AND guild_id = ?",
                (event_id, ctx.guild.id)
            )
            row = await cursor.fetchone()
            if not row:
                return await ctx.reply("Event not found.")
            name, status = row
            if status != "scheduled":
                return await ctx.reply("Only scheduled events can be canceled.")
            await db.execute(
                "UPDATE events SET status = 'cancelled' WHERE event_id = ?",
                (event_id,)
            )
            await db.commit()
        await ctx.reply(f"Canceled **{name}**.")


async def setup(bot: commands.Bot):
    await bot.add_cog(SeasonalEvents(bot))
