"""유튜브 새 영상 / 생방송 알림 디스코드 봇 (개인 알림방 방식).

- 서버에 '🔔-알림신청' 방이 생기고, '내 알림방 만들기' 버튼을 누르면 나만 보이는 방이 생겨요.
- 내 방에서 '➕ 유튜버 추가' 버튼으로 링크를 넣으면, 그 유튜버의 새 영상·생방송 알림이 내 방에만 와요.
- 구글 로그인이나 API 키 없이, 유튜브 RSS와 영상 페이지만 읽어서 동작해요.
"""

import asyncio
import hashlib
import io
import json
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import aiohttp
import discord
from discord import app_commands
from discord.ext import tasks
from dotenv import load_dotenv

load_dotenv()


def env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


DISCORD_TOKEN = env("DISCORD_TOKEN")
DISCORD_GUILD_ID = int(env("DISCORD_GUILD_ID", "0"))
CATEGORY_NAME = env("CATEGORY_NAME", "유튜브 알림")
PANEL_CHANNEL_NAME = env("PANEL_CHANNEL_NAME", "🔔-알림신청")
STORAGE_CHANNEL_NAME = env("STORAGE_CHANNEL_NAME", "🔒-봇-저장소")
STORAGE_FILENAME = "state.json"
ROOM_PREFIX = env("ROOM_PREFIX", "🔔-")
CHECK_INTERVAL = int(env("CHECK_INTERVAL", "300"))
MAX_PER_USER = int(env("MAX_CHANNELS_PER_USER", "30"))

# 저장 위치: 직접 정한 곳 > Railway 저장 공간(/data) > 봇 파일 옆
_DEFAULT_STATE = Path("/data/state.json") if Path("/data").is_dir() else Path(__file__).with_name("state.json")
STATE_FILE = Path(env("STATE_FILE") or _DEFAULT_STATE)

RSS_URL = "https://www.youtube.com/feeds/videos.xml?channel_id={}"
WATCH_URL = "https://www.youtube.com/watch?v={}"
NS = {"atom": "http://www.w3.org/2005/Atom", "yt": "http://www.youtube.com/xml/schemas/2015"}
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
}
COOKIES = {"CONSENT": "YES+cb", "SOCS": "CAI"}
CHANNEL_ID_RE = re.compile(r"UC[\w-]{22}")

RED = 0xFF0000
ORANGE = 0xFFA500
GRAY = 0x808080
BLUE = 0x5865F2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("yt-notify-bot")


# ---------- 상태 저장 ----------
def load_state() -> dict:
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------- 유튜브 페이지 읽기 ----------
def parse_feed(text: str) -> tuple[str, list[dict]]:
    """RSS → (채널 이름, 최신 영상 목록)."""
    root = ET.fromstring(text)
    name = root.findtext("atom:title", namespaces=NS) or ""
    videos = []
    for entry in root.findall("atom:entry", NS):
        vid = entry.findtext("yt:videoId", namespaces=NS)
        if vid:
            videos.append({"id": vid, "title": entry.findtext("atom:title", namespaces=NS) or ""})
    return name, videos


def extract_channel_id(page: str) -> str | None:
    """유튜브 채널/영상 페이지 HTML에서 채널 ID(UC...)를 찾아요."""
    patterns = [
        r'<link rel="canonical" href="https://www\.youtube\.com/channel/(UC[\w-]{22})"',
        r'"externalId":"(UC[\w-]{22})"',
        r'<meta itemprop="(?:identifier|channelId)" content="(UC[\w-]{22})"',
        r'"channelId":"(UC[\w-]{22})"',
    ]
    for p in patterns:
        m = re.search(p, page)
        if m:
            return m.group(1)
    return None


def parse_watch(page: str) -> dict:
    """영상 페이지 HTML → {"status": live|upcoming|ended|none, "start": 예정 시각(unix)}."""
    if '"isLiveNow":true' in page:
        return {"status": "live", "start": None}
    if '"isUpcoming":true' in page or '"upcomingEventData"' in page:
        m = re.search(r'"upcomingEventData":\{"startTime":"(\d+)"', page) or re.search(
            r'"scheduledStartTime":"(\d+)"', page
        )
        return {"status": "upcoming", "start": int(m.group(1)) if m else None}
    if '"isLiveContent":true' in page:
        return {"status": "ended", "start": None}
    return {"status": "none", "start": None}


def normalize_link(text: str) -> str:
    s = text.strip().strip("<>")
    if s.startswith("@"):
        return f"https://www.youtube.com/{s}"
    if "youtube.com" in s or "youtu.be" in s:
        return s if s.startswith("http") else "https://" + s
    return f"https://www.youtube.com/@{s}"


class YouTube:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def get(self, url: str) -> str | None:
        try:
            async with self.session.get(
                url, headers=HEADERS, cookies=COOKIES, timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.status != 200:
                    log.warning("유튜브 페이지를 못 불러왔어요 (%s): HTTP %s", url, resp.status)
                    return None
                return await resp.text()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning("유튜브 페이지를 못 불러왔어요 (%s): %s", url, e)
            return None

    async def feed(self, channel_id: str) -> tuple[str, list[dict]] | None:
        text = await self.get(RSS_URL.format(channel_id))
        if text is None:
            return None
        try:
            return parse_feed(text)
        except ET.ParseError:
            return None

    async def video_status(self, video_id: str) -> dict | None:
        page = await self.get(WATCH_URL.format(video_id))
        return parse_watch(page) if page else None

    async def resolve(self, text: str) -> tuple[str, str, list[dict]]:
        """링크/핸들 → (채널ID, 채널 이름, 최신 영상). 못 찾으면 ValueError."""
        m = CHANNEL_ID_RE.search(text)
        cid = m.group(0) if m else None
        if cid is None:
            page = await self.get(normalize_link(text))
            cid = extract_channel_id(page) if page else None
        if cid is None:
            raise ValueError("유튜브 채널을 찾지 못했어요. 채널 링크(예: https://www.youtube.com/@이름)를 넣어 주세요.")
        feed = await self.feed(cid)
        if feed is None:
            raise ValueError("채널은 찾았는데 영상 목록을 못 불러왔어요. 잠시 뒤에 다시 해 주세요.")
        name, videos = feed
        return cid, name or cid, videos


def thumbnail(video_id: str) -> str:
    return f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"


# ---------- 디스코드 봇 ----------
class NotifyBot(discord.Client):
    def __init__(self):
        super().__init__(intents=discord.Intents.default())
        self.tree = app_commands.CommandTree(self)
        self.state = self._with_defaults(load_state())
        self.http_session: aiohttp.ClientSession | None = None
        self.yt: YouTube | None = None
        self._lock = asyncio.Lock()
        self._synced = False
        # 디스코드 저장소 (어떤 호스팅을 써도 기록이 안 사라지게)
        self._loaded = asyncio.Event()
        self._store_msg: discord.Message | None = None
        self._saved_hash = ""
        self._dirty = False

    @staticmethod
    def _with_defaults(state: dict) -> dict:
        state.setdefault("subs", {})  # {유튜브채널ID: {"name": 이름, "users": [디스코드유저ID...]}}
        state.setdefault("rooms", {})  # {디스코드유저ID(str): 알림방ID}
        state.setdefault("seen", {})  # {유튜브채널ID: [본 영상ID...]}
        state.setdefault("upcoming", {})  # {영상ID: {"cid", "title", "added"}}
        state.setdefault("panel", {})  # {"channel": ID, "message": ID}
        return state

    def save(self) -> None:
        """파일에 바로 저장하고, 디스코드 저장소에는 잠시 뒤 모아서 올려요."""
        save_state(self.state)
        self._dirty = True

    def _dump(self) -> bytes:
        return json.dumps(self.state, ensure_ascii=False, indent=1).encode("utf-8")

    async def _storage_channel(self, guild: discord.Guild) -> discord.TextChannel:
        ch = discord.utils.get(guild.text_channels, name=STORAGE_CHANNEL_NAME)
        if ch is None:
            ch = await guild.create_text_channel(
                STORAGE_CHANNEL_NAME,
                category=await self.ensure_category(guild),
                topic="유튜브 알림 봇의 저장 공간이에요. 이 방과 메시지를 지우지 마세요!",
                overwrites={
                    guild.default_role: discord.PermissionOverwrite(view_channel=False),
                    guild.me: discord.PermissionOverwrite(
                        view_channel=True, send_messages=True, attach_files=True, read_message_history=True
                    ),
                },
                reason="유튜브 알림 봇 저장소",
            )
            log.info("#%s 방을 만들었어요.", ch.name)
        return ch

    async def _new_store_message(self, guild: discord.Guild) -> None:
        ch = await self._storage_channel(guild)
        data = self._dump()
        self._store_msg = await ch.send(
            "⚠️ 유튜브 알림 봇이 쓰는 저장 공간이에요. **이 메시지를 지우지 마세요!**",
            file=discord.File(io.BytesIO(data), STORAGE_FILENAME),
        )
        self._saved_hash = hashlib.sha256(data).hexdigest()

    async def load_remote_state(self) -> None:
        """디스코드 '🔒-봇-저장소' 방에서 기록을 불러와요. 없으면 새로 만들어요."""
        guild = self.guild()
        if guild is None:
            return
        ch = await self._storage_channel(guild)
        async for msg in ch.history(limit=50):
            att = next((a for a in msg.attachments if a.filename == STORAGE_FILENAME), None)
            if msg.author != guild.me or att is None:
                continue
            try:
                remote = json.loads((await att.read()).decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError, discord.HTTPException):
                log.exception("저장소 기록을 읽지 못했어요.")
                continue
            self.state = self._with_defaults(remote)
            save_state(self.state)
            self._store_msg = msg
            self._saved_hash = hashlib.sha256(self._dump()).hexdigest()
            log.info("디스코드 저장소에서 기록을 불러왔어요. (유튜버 %d명)", len(self.state["subs"]))
            return
        await self._new_store_message(guild)  # 저장된 기록이 없으면 지금 상태로 새로 만들어요

    async def flush_remote(self) -> None:
        """바뀐 내용이 있으면 디스코드 저장소 메시지의 파일을 바꿔요."""
        if not self._dirty or not self._loaded.is_set():
            return
        data = self._dump()
        digest = hashlib.sha256(data).hexdigest()
        self._dirty = False
        if digest == self._saved_hash:
            return
        try:
            if self._store_msg is None:
                guild = self.guild()
                if guild is not None:
                    await self._new_store_message(guild)
                return
            await self._store_msg.edit(attachments=[discord.File(io.BytesIO(data), STORAGE_FILENAME)])
            self._saved_hash = digest
        except discord.NotFound:
            log.warning("저장소 메시지가 지워져서 다시 만들게요.")
            self._store_msg = None
            self._dirty = True
        except discord.HTTPException:
            self._dirty = True
            log.exception("디스코드 저장소에 저장하지 못했어요. 잠시 뒤 다시 시도할게요.")

    @tasks.loop(seconds=30)
    async def flush_loop(self):
        try:
            await self.flush_remote()
        except Exception:
            log.exception("저장 중 오류가 났어요.")

    async def setup_hook(self):
        self.http_session = aiohttp.ClientSession()
        self.yt = YouTube(self.http_session)
        self.add_view(PanelView())
        self.add_view(RoomView())
        self.check_loop.change_interval(seconds=CHECK_INTERVAL)
        self.check_loop.start()
        self.flush_loop.start()

    async def close(self):
        try:
            await self.flush_remote()
        except Exception:
            log.exception("마지막 저장에 실패했어요.")
        if self.http_session:
            await self.http_session.close()
        await super().close()

    async def on_ready(self):
        log.info("%s 로 로그인했어요. 확인 주기: %d초", self.user, CHECK_INTERVAL)
        if not self._synced:
            for g in self.guilds:  # 서버별로 등록하면 명령어가 바로 보여요
                self.tree.copy_global_to(guild=g)
                await self.tree.sync(guild=g)
            self._synced = True
        if not self._loaded.is_set():
            try:
                await self.load_remote_state()
            except discord.Forbidden:
                log.error("저장소 방을 만들 권한이 없어요. 봇 역할에 '관리자' 권한을 주세요.")
            except Exception:
                log.exception("디스코드 저장소를 준비하지 못했어요. 파일 저장만 쓸게요.")
            self._loaded.set()
        try:
            await self.ensure_panel()
        except discord.Forbidden:
            log.error("알림신청 방을 만들 권한이 없어요. 봇 역할에 '관리자' 권한을 주세요.")
        except Exception:
            log.exception("알림신청 방을 준비하지 못했어요.")

    # --- 서버/방 준비 ---
    def guild(self) -> discord.Guild | None:
        if DISCORD_GUILD_ID:
            return self.get_guild(DISCORD_GUILD_ID)
        return self.guilds[0] if self.guilds else None

    async def ensure_category(self, guild: discord.Guild) -> discord.CategoryChannel:
        cat = discord.utils.get(guild.categories, name=CATEGORY_NAME)
        return cat or await guild.create_category(CATEGORY_NAME, reason="유튜브 알림 봇")

    async def ensure_panel(self) -> None:
        guild = self.guild()
        if guild is None:
            return
        panel = self.state["panel"]
        ch = guild.get_channel(panel.get("channel", 0)) or discord.utils.get(guild.text_channels, name=PANEL_CHANNEL_NAME)
        if ch is None:
            cat = await self.ensure_category(guild)
            ch = await guild.create_text_channel(
                PANEL_CHANNEL_NAME,
                category=cat,
                overwrites={
                    guild.default_role: discord.PermissionOverwrite(view_channel=True, send_messages=False),
                    guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, embed_links=True),
                },
                reason="유튜브 알림 봇",
            )
            log.info("#%s 방을 만들었어요.", ch.name)
        if panel.get("channel") == ch.id and panel.get("message"):
            try:
                await ch.fetch_message(panel["message"])
                return  # 안내 메시지가 이미 있어요
            except discord.NotFound:
                pass
        embed = discord.Embed(
            title="🔔 유튜브 알림 신청",
            description=(
                "좋아하는 유튜버의 **새 영상**과 **생방송** 알림을 받아 보세요!\n\n"
                "1. 아래 **내 알림방 만들기** 버튼을 눌러요.\n"
                "2. 나만 보이는 알림방이 생겨요.\n"
                "3. 내 방에서 **➕ 유튜버 추가**를 누르고 유튜브 링크를 붙여넣으면 끝!"
            ),
            color=BLUE,
        )
        msg = await ch.send(embed=embed, view=PanelView())
        self.state["panel"] = {"channel": ch.id, "message": msg.id}
        self.save()

    async def get_room(self, member: discord.Member, create: bool = True) -> discord.TextChannel | None:
        """그 사람만 보이는 알림방. 없으면 만들어요."""
        await self._loaded.wait()
        guild = member.guild
        room = guild.get_channel(self.state["rooms"].get(str(member.id), 0))
        if room is not None or not create:
            return room  # type: ignore[return-value]
        cat = await self.ensure_category(guild)
        room = await guild.create_text_channel(
            f"{ROOM_PREFIX}{member.display_name}"[:90],
            category=cat,
            topic=f"{member.display_name} 님의 유튜브 알림방이에요. 다른 사람은 볼 수 없어요.",
            overwrites={
                guild.default_role: discord.PermissionOverwrite(view_channel=False),
                member: discord.PermissionOverwrite(
                    view_channel=True, send_messages=True, read_message_history=True, use_application_commands=True
                ),
                guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, embed_links=True),
            },
            reason=f"{member} 님의 유튜브 알림방",
        )
        self.state["rooms"][str(member.id)] = room.id
        self.save()
        embed = discord.Embed(
            title="🎉 내 알림방이 만들어졌어요!",
            description=(
                "여기는 **나만 보이는 방**이에요.\n\n"
                "**➕ 유튜버 추가**: 유튜브 채널 링크를 넣으면 알림을 받아요\n"
                "**📋 내 목록**: 알림 받는 유튜버 보기\n"
                "**➖ 삭제**: 알림 그만 받기\n\n"
                "새 영상은 🎬, 생방송은 🔴 로 알려 드릴게요!"
            ),
            color=BLUE,
        )
        await room.send(content=member.mention, embed=embed, view=RoomView())
        log.info("%s 님의 알림방 #%s 을 만들었어요.", member, room.name)
        return room

    # --- 구독 관리 ---
    def user_channels(self, uid: int) -> list[tuple[str, str]]:
        return sorted(
            ((cid, s["name"]) for cid, s in self.state["subs"].items() if uid in s["users"]), key=lambda x: x[1]
        )

    def add_sub(self, uid: int, cid: str, name: str, videos: list[dict]) -> bool:
        sub = self.state["subs"].setdefault(cid, {"name": name, "users": []})
        sub["name"] = name
        # 처음 추가하는 채널이면 지금 있는 영상은 '이미 본 것'으로 기록 (옛날 영상 알림 방지)
        self.state["seen"].setdefault(cid, [v["id"] for v in videos])
        if uid in sub["users"]:
            return False
        sub["users"].append(uid)
        self.save()
        return True

    def remove_sub(self, uid: int, cid: str) -> str | None:
        sub = self.state["subs"].get(cid)
        if not sub or uid not in sub["users"]:
            return None
        sub["users"].remove(uid)
        name = sub["name"]
        if not sub["users"]:  # 아무도 안 보는 채널은 정리
            self.state["subs"].pop(cid)
            self.state["seen"].pop(cid, None)
        self.save()
        return name

    async def handle_add(self, interaction: discord.Interaction, link: str) -> None:
        member = interaction.user
        assert isinstance(member, discord.Member) and self.yt is not None
        await self._loaded.wait()
        if len(self.user_channels(member.id)) >= MAX_PER_USER:
            await interaction.followup.send(f"한 사람당 유튜버는 {MAX_PER_USER}명까지 추가할 수 있어요.", ephemeral=True)
            return
        try:
            cid, name, videos = await self.yt.resolve(link)
        except ValueError as e:
            await interaction.followup.send(f"😢 {e}", ephemeral=True)
            return
        async with self._lock:
            added = self.add_sub(member.id, cid, name, videos)
        room = await self.get_room(member)
        if added:
            await interaction.followup.send(
                f"✅ **{name}** 추가 완료! 이제 새 영상과 생방송을 {room.mention} 에 알려 드릴게요.", ephemeral=True
            )
        else:
            await interaction.followup.send(f"**{name}** 은(는) 이미 추가되어 있어요.", ephemeral=True)

    # --- 주기 확인 ---
    @tasks.loop(seconds=300)
    async def check_loop(self):
        try:
            await self.check_once()
        except Exception:
            log.exception("확인 중 오류가 났어요. 다음 주기에 다시 시도할게요.")

    @check_loop.before_loop
    async def before_check(self):
        await self.wait_until_ready()
        await self._loaded.wait()

    async def check_once(self):
        async with self._lock:
            assert self.yt is not None
            subs: dict = self.state["subs"]
            sem = asyncio.Semaphore(8)

            async def fetch(cid: str):
                async with sem:
                    return cid, await self.yt.feed(cid)

            results = await asyncio.gather(*(fetch(cid) for cid in list(subs)))

            new_videos: list[tuple[str, dict]] = []
            for cid, feed in results:
                if feed is None or cid not in subs:
                    continue
                name, videos = feed
                if name:
                    subs[cid]["name"] = name
                seen = self.state["seen"].get(cid)
                if seen is None:
                    self.state["seen"][cid] = [v["id"] for v in videos]
                    continue
                seen_set = set(seen)
                fresh = [v for v in videos if v["id"] not in seen_set]
                new_videos.extend((cid, v) for v in reversed(fresh))  # 오래된 것부터
                self.state["seen"][cid] = ([v["id"] for v in videos] + seen)[:50]

            async def status(vid: str):
                async with sem:
                    return await self.yt.video_status(vid)

            # 새 영상 알림
            new_ids = {v["id"] for _, v in new_videos}
            statuses = await asyncio.gather(*(status(v["id"]) for _, v in new_videos))
            for (cid, v), st in zip(new_videos, statuses):
                kind = st["status"] if st else "unknown"
                if kind == "upcoming":
                    self.state["upcoming"][v["id"]] = {"cid": cid, "title": v["title"], "added": time.time()}
                await self.notify(cid, v["id"], v["title"], kind, st["start"] if st else None)

            # 예정됐던 생방송이 시작됐는지
            waiting = [vid for vid in self.state["upcoming"] if vid not in new_ids]
            statuses = await asyncio.gather(*(status(vid) for vid in waiting))
            for vid, st in zip(waiting, statuses):
                info = self.state["upcoming"][vid]
                if st and st["status"] == "live":
                    await self.notify(info["cid"], vid, info["title"], "live")
                    self.state["upcoming"].pop(vid)
                elif (st and st["status"] in ("none", "ended")) or time.time() - info["added"] > 14 * 86400:
                    self.state["upcoming"].pop(vid)

            self.save()

    async def notify(self, cid: str, vid: str, title: str, kind: str, start: int | None = None) -> None:
        sub = self.state["subs"].get(cid)
        if not sub:
            return
        name = sub["name"]
        text = {
            "live": (f"🔴 **{name}** 님이 생방송을 시작했어요!", RED),
            "upcoming": (f"⏰ **{name}** 님의 생방송(또는 최초 공개)이 예정됐어요!", ORANGE),
            "ended": (f"📼 **{name}** 님의 지난 생방송 다시보기가 올라왔어요", GRAY),
            "none": (f"🎬 **{name}** 님의 새 영상이 올라왔어요!", RED),
        }.get(kind, (f"🎬 **{name}** 님의 새 영상(또는 생방송)이 올라왔어요!", RED))
        desc, color = text
        if kind == "upcoming" and start:
            desc += f"\n시작 예정: <t:{start}:F> (<t:{start}:R>)"
        embed = discord.Embed(title=title, url=WATCH_URL.format(vid), description=desc, color=color)
        embed.set_image(url=thumbnail(vid))
        for uid in list(sub["users"]):
            await self.deliver(uid, embed)

    async def deliver(self, uid: int, embed: discord.Embed) -> None:
        guild = self.guild()
        if guild is None:
            return
        room = guild.get_channel(self.state["rooms"].get(str(uid), 0))
        if room is None:
            try:
                member = guild.get_member(uid) or await guild.fetch_member(uid)
            except discord.NotFound:
                # 서버를 나간 사람: 구독 정리
                for cid in [c for c, s in self.state["subs"].items() if uid in s["users"]]:
                    self.remove_sub(uid, cid)
                self.state["rooms"].pop(str(uid), None)
                return
            room = await self.get_room(member)
        try:
            await room.send(content=f"<@{uid}>", embed=embed, allowed_mentions=discord.AllowedMentions(users=True))
        except discord.HTTPException:
            log.exception("알림을 보내지 못했어요 (유저 %s)", uid)


bot = NotifyBot()


# ---------- 버튼 / 입력창 ----------
class SafeView(discord.ui.View):
    async def on_error(self, interaction: discord.Interaction, error: Exception, item) -> None:
        log.exception("버튼 처리 중 오류", exc_info=error)
        msg = "😢 오류가 났어요. 봇에게 '관리자' 권한이 있는지 확인해 주세요."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.HTTPException:
            pass


class PanelView(SafeView):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="내 알림방 만들기", emoji="🔔", style=discord.ButtonStyle.primary, custom_id="ytn:open_room")
    async def open_room(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        room = await bot.get_room(interaction.user)  # type: ignore[arg-type]
        await interaction.followup.send(f"👉 내 알림방은 여기예요: {room.mention}", ephemeral=True)


class AddModal(discord.ui.Modal, title="유튜버 추가"):
    link = discord.ui.TextInput(
        label="유튜브 채널 링크",
        placeholder="https://www.youtube.com/@유튜버이름 (영상 링크도 돼요)",
        max_length=200,
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        await bot.handle_add(interaction, str(self.link.value))


class RemoveSelect(discord.ui.Select):
    def __init__(self, channels: list[tuple[str, str]]):
        options = [discord.SelectOption(label=name[:100], value=cid) for cid, name in channels[:25]]
        super().__init__(placeholder="알림을 그만 받을 유튜버를 골라 주세요", options=options)

    async def callback(self, interaction: discord.Interaction):
        async with bot._lock:
            name = bot.remove_sub(interaction.user.id, self.values[0])
        msg = f"➖ **{name}** 알림을 껐어요." if name else "이미 삭제된 유튜버예요."
        await interaction.response.edit_message(content=msg, view=None)


class RoomView(SafeView):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="유튜버 추가", emoji="➕", style=discord.ButtonStyle.success, custom_id="ytn:add")
    async def add(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(AddModal())

    @discord.ui.button(label="내 목록", emoji="📋", style=discord.ButtonStyle.secondary, custom_id="ytn:list")
    async def list_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await send_list(interaction)

    @discord.ui.button(label="삭제", emoji="➖", style=discord.ButtonStyle.danger, custom_id="ytn:remove")
    async def remove(self, interaction: discord.Interaction, button: discord.ui.Button):
        await send_remove(interaction)


async def send_list(interaction: discord.Interaction):
    chans = bot.user_channels(interaction.user.id)
    if not chans:
        await interaction.response.send_message("아직 추가한 유튜버가 없어요. **➕ 유튜버 추가**를 눌러 보세요!", ephemeral=True)
        return
    lines = "\n".join(f"• [{name}](https://www.youtube.com/channel/{cid})" for cid, name in chans)
    await interaction.response.send_message(f"📋 알림 받는 유튜버 **{len(chans)}명**\n{lines}"[:1990], ephemeral=True)


async def send_remove(interaction: discord.Interaction):
    chans = bot.user_channels(interaction.user.id)
    if not chans:
        await interaction.response.send_message("삭제할 유튜버가 없어요.", ephemeral=True)
        return
    view = SafeView(timeout=180)
    view.add_item(RemoveSelect(chans))
    await interaction.response.send_message("누구 알림을 끌까요?", view=view, ephemeral=True)


# ---------- 명령어 (버튼과 같은 기능) ----------
@bot.tree.command(name="채널추가", description="알림 받을 유튜버를 추가해요")
@app_commands.describe(링크="유튜브 채널 링크 (예: https://www.youtube.com/@이름)")
@app_commands.guild_only()
async def add_cmd(interaction: discord.Interaction, 링크: str):
    await interaction.response.defer(ephemeral=True, thinking=True)
    await bot.handle_add(interaction, 링크)


@bot.tree.command(name="채널목록", description="내가 알림 받는 유튜버를 보여 줘요")
@app_commands.guild_only()
async def list_cmd(interaction: discord.Interaction):
    await send_list(interaction)


@bot.tree.command(name="채널삭제", description="유튜버 알림을 그만 받아요")
@app_commands.guild_only()
async def remove_cmd(interaction: discord.Interaction):
    await send_remove(interaction)


@bot.tree.command(name="알림방", description="내 알림방으로 가는 링크를 알려 줘요 (없으면 만들어요)")
@app_commands.guild_only()
async def room_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    room = await bot.get_room(interaction.user)  # type: ignore[arg-type]
    await interaction.followup.send(f"👉 내 알림방은 여기예요: {room.mention}", ephemeral=True)


@bot.tree.error
async def on_app_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    log.exception("명령어 처리 중 오류", exc_info=error)
    msg = "😢 오류가 났어요. 봇에게 '관리자' 권한이 있는지 확인해 주세요."
    if interaction.response.is_done():
        await interaction.followup.send(msg, ephemeral=True)
    else:
        await interaction.response.send_message(msg, ephemeral=True)


def main():
    if not DISCORD_TOKEN:
        raise SystemExit("DISCORD_TOKEN 환경 변수가 비어 있어요. (README 참고)")
    bot.run(DISCORD_TOKEN, log_handler=None)


if __name__ == "__main__":
    main()
