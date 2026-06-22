import asyncio
import os
import re
import aiohttp
from collections import deque
from telethon import TelegramClient, events
from telethon.tl.types import MessageMediaPhoto
from dotenv import load_dotenv
from aiohttp import web

load_dotenv()

# ─── CONFIG ───────────────────────────────────────────────────────────────────
API_ID          = int(os.getenv("API_ID"))
API_HASH        = os.getenv("API_HASH")
PHONE           = os.getenv("PHONE")

BOT2_USERNAME   = "@TeraBoxDownloader_TgBot"
BOT3_USERNAME   = "@aishwariyaupdatesbot"
BOT2_CHANNEL_ID = int(os.getenv("BOT2_CHANNEL_ID", "-1003968408263"))
OUTPUT_CHANNEL  = int(os.getenv("OUTPUT_CHANNEL", "-1004298614570"))
PORT            = int(os.getenv("PORT", "8080"))

CATBOX_API     = "https://catbox.moe/user/api.php"
STREAM_PATTERN = re.compile(r"https://stream\.bhabhiji\.fun/watch/[^\s]+")
TERABOX_PATTERN = re.compile(
    r"https?://(?:www\.)?"
    r"(?:"
    r"terabox\.com"
    r"|1024terabox\.com"
    r"|teraboxapp\.com"
    r"|terabox\.app"
    r"|nephobox\.com"
    r"|mirrorbox\.com"
    r"|momerybox\.com"
    r"|freeterabox\.com"
    r"|teraboxlink\.com"
    r"|4funbox\.com"
    r"|terafileshare\.com"
    r"|teraboxshare\.com"
    r"|terasharelink\.com"
    r")"
    r"[^\s]*",
    re.IGNORECASE
)

# ─── STATE ────────────────────────────────────────────────────────────────────
queue       = deque()
processing  = False
current_job = None

client = TelegramClient("userbot_session", API_ID, API_HASH)

# ─── DUMMY WEB SERVER (Render ke liye) ────────────────────────────────────────

async def handle(request):
    return web.Response(text="Bot is running ✅")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"[Web] Server running on port {PORT}")

# ─── HELPERS ──────────────────────────────────────────────────────────────────

async def upload_to_catbox(image_bytes: bytes, filename: str = "image.jpg"):
    try:
        data = aiohttp.FormData()
        data.add_field("reqtype", "fileupload")
        data.add_field("fileToUpload", image_bytes, filename=filename, content_type="image/jpeg")
        async with aiohttp.ClientSession() as session:
            async with session.post(CATBOX_API, data=data, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status == 200:
                    url = (await resp.text()).strip()
                    if url.startswith("https://"):
                        return url
    except Exception as e:
        print(f"[Catbox] Error: {e}")
    return None


async def download_first_image(message):
    try:
        if message.photo or (message.media and isinstance(message.media, MessageMediaPhoto)):
            return await client.download_media(message, bytes)
    except Exception as e:
        print(f"[Download Image] Error: {e}")
    return None


def extract_first_terabox_link(text: str):
    if not text:
        return None
    match = TERABOX_PATTERN.search(text)
    return match.group(0) if match else None


def extract_stream_url(text: str):
    if not text:
        return None
    match = STREAM_PATTERN.search(text)
    return match.group(0) if match else None


async def send_result(catbox_url: str, stream_url: str, title: str = ""):
    msg = (
        f"✅ **Processed**\n\n"
        f"{'📌 **Title:** ' + title + chr(10) if title else ''}"
        f"🖼 **Catbox:** {catbox_url}\n\n"
        f"🎬 **Stream:** {stream_url}"
    )
    await client.send_message(OUTPUT_CHANNEL, msg)


# ─── QUEUE PROCESSOR ──────────────────────────────────────────────────────────

async def process_queue():
    global processing, current_job

    if processing:
        return

    while queue:
        processing = True
        job = queue.popleft()
        current_job = job
        print(f"[Queue] Processing job: {job.get('title', 'Unknown')}")

        try:
            await process_job(job)
        except Exception as e:
            print(f"[Queue] Job failed: {e}")
            await client.send_message(OUTPUT_CHANNEL, f"❌ Job failed: {e}\nTitle: {job.get('title', '')}")

        current_job = None

    processing = False


async def process_job(job: dict):
    title       = job.get("title", "")
    image_bytes = job.get("image_bytes")
    tera_link   = job.get("tera_link")

    # Step 1: Catbox upload
    catbox_url = None
    if image_bytes:
        print("[Step 1] Uploading to Catbox...")
        catbox_url = await upload_to_catbox(image_bytes)
        print(f"[Step 1] Catbox URL: {catbox_url}")
    else:
        print("[Step 1] No image, skipping Catbox.")

    # Step 2: Send Terabox link to Bot2
    print(f"[Step 2] Sending link to Bot2: {tera_link}")
    await client.send_message(BOT2_USERNAME, tera_link)

    # Step 3: Wait for Bot2 channel video
    print("[Step 3] Waiting for Bot2 channel video...")
    bot2_future = asyncio.get_event_loop().create_future()
    job["bot2_future"] = bot2_future

    try:
        bot2_video_msg = await asyncio.wait_for(bot2_future, timeout=180)
    except asyncio.TimeoutError:
        raise Exception("Bot2 channel video not received within 3 minutes.")

    print(f"[Step 3] Video received: msg_id={bot2_video_msg.id}")

    # Step 4: Forward to Bot3
    print("[Step 4] Forwarding to Bot3...")
    bot3_entity = await client.get_entity(BOT3_USERNAME)
    await client.forward_messages(bot3_entity, bot2_video_msg)

    # Step 5: Wait for Bot3 stream URL
    print("[Step 5] Waiting for Bot3 stream URL...")
    bot3_future = asyncio.get_event_loop().create_future()
    job["bot3_future"] = bot3_future

    try:
        stream_url = await asyncio.wait_for(bot3_future, timeout=120)
    except asyncio.TimeoutError:
        raise Exception("Bot3 stream URL not received within 2 minutes.")

    print(f"[Step 5] Stream URL: {stream_url}")

    # Step 6: Send result
    if not catbox_url:
        catbox_url = "N/A (no image)"
    await send_result(catbox_url, stream_url, title)
    print("[Step 6] Done!")


# ─── EVENT HANDLERS ───────────────────────────────────────────────────────────

@client.on(events.NewMessage(incoming=True, from_users="me"))
async def on_forwarded_post(event):
    if not event.message.fwd_from:
        return

    msg  = event.message
    text = msg.text or msg.caption or ""

    tera_link = extract_first_terabox_link(text)
    if not tera_link:
        print("[Received] No Terabox link found, skipping.")
        return

    image_bytes = await download_first_image(msg)

    title = ""
    if text:
        lines = text.strip().split("\n")
        title = lines[0][:100] if lines else ""

    job = {
        "title":       title,
        "image_bytes": image_bytes,
        "tera_link":   tera_link,
        "bot2_future": None,
        "bot3_future": None,
    }

    queue.append(job)
    print(f"[Queue] Added job. Queue size: {len(queue)}")
    asyncio.create_task(process_queue())


@client.on(events.NewMessage(chats=BOT2_CHANNEL_ID))
async def on_bot2_channel_video(event):
    global current_job
    msg = event.message

    if not msg.video and not (msg.document and "video" in (msg.document.mime_type or "")):
        return

    print(f"[Bot2 Channel] Video received: msg_id={msg.id}")

    if current_job and current_job.get("bot2_future") and not current_job["bot2_future"].done():
        current_job["bot2_future"].set_result(msg)
    else:
        print("[Bot2 Channel] No pending job, ignoring.")


@client.on(events.NewMessage(incoming=True))
async def on_bot3_reply(event):
    global current_job

    try:
        sender = await event.get_sender()
    except Exception:
        return

    if not sender:
        return
    username = getattr(sender, "username", None)
    if not username:
        return
    if username.lower() != "aishwariyaupdatesbot":
        return

    msg  = event.message
    text = msg.text or msg.caption or ""
    stream_url = extract_stream_url(text)

    if not stream_url:
        return

    print(f"[Bot3] Stream URL: {stream_url}")

    if current_job and current_job.get("bot3_future") and not current_job["bot3_future"].done():
        current_job["bot3_future"].set_result(stream_url)
    else:
        print("[Bot3] No pending job, ignoring.")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

async def main():
    print("🚀 Userbot starting...")
    await start_web_server()
    await client.start(phone=PHONE)
    me = await client.get_me()
    print(f"✅ Logged in as: {me.first_name} (@{me.username})")
    print("📡 Listening for forwarded posts...")
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
