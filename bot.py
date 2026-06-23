import os
import json
import time
import random
import asyncio
import aiohttp
import logging
import tempfile
from dotenv import load_dotenv

load_dotenv()

from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
import firebase_admin
from firebase_admin import credentials, db

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
BOT_TOKEN      = os.environ["BOT_TOKEN"]
API_ID         = int(os.environ["API_ID"])
API_HASH       = os.environ["API_HASH"]
ADMIN_ID       = int(os.environ["ADMIN_ID"])
CHANNEL_ID     = int(os.environ["CHANNEL_ID"])
FIREBASE_URL   = os.environ["FIREBASE_URL"]
PIXELDRAIN_KEY = os.environ["PIXELDRAIN_API_KEY"]
SESSION_STRING = os.environ["SESSION_STRING"]
TERABOX_BOT    = os.environ.get("TERABOX_BOT", "@TeraBoxDownloader_TgBot")

FIREBASE_CRED_JSON = os.environ.get("FIREBASE_CRED_JSON", "")
FIREBASE_CRED_PATH = os.environ.get("FIREBASE_CRED_PATH", "serviceAccountKey.json")

CATBOX_URL     = "https://catbox.moe/user/api.php"
PIXELDRAIN_URL = "https://pixeldrain.com/api/file"

TERABOX_DOMAINS = [
    "terabox.com", "1024terabox.com", "terafileshare.com",
    "terasharelink.com", "teraboxapp.com", "terabox.app"
]

CAPTIONS = [
    "Ekdum Mast Content! Dekhte raho...",
    "Aaj ka sabse hot upload!",
    "Itna spicy content pehle kabhi nahi dekha!",
    "Dhamaka content! Miss mat karna...",
    "Premium quality, free mein enjoy karo!",
    "Ye dekh ke pagal ho jaoge!",
    "Sirf adults ke liye — 18+ content!",
    "Aaj raat ke liye perfect entertainment!",
    "Full masti, full entertainment!",
    "Popcorn lo aur enjoy karo!",
    "Bhabhi ka naya jawab nahi!",
    "Devar bhabhi ka dhamakedar scene!",
    "Aaj ki raat rangeen hogi!",
    "Ye video dekhe bina mat sona!",
    "Seedha dil pe lagega yeh content!",
]

# ── Firebase ──────────────────────────────────────────────────────────────────
if FIREBASE_CRED_JSON:
    cred = credentials.Certificate(json.loads(FIREBASE_CRED_JSON))
else:
    cred = credentials.Certificate(FIREBASE_CRED_PATH)
firebase_admin.initialize_app(cred, {"databaseURL": FIREBASE_URL})

# ── Queue ─────────────────────────────────────────────────────────────────────
task_queue: asyncio.Queue = asyncio.Queue()
is_processing: bool = False
current_video_event: asyncio.Event = asyncio.Event()
current_video_media = None

# ── Firebase helpers ──────────────────────────────────────────────────────────
def get_post_number(post_id: str) -> int:
    try:
        posts = db.reference("posts").get() or {}
        sorted_ids = sorted(posts.keys(), key=lambda k: posts[k].get("order", 0), reverse=True)
        return sorted_ids.index(post_id) + 1 if post_id in sorted_ids else 0
    except:
        return 0

def save_post(video_url: str, image_url: str, caption: str, premium: bool = False) -> str:
    post = db.reference("posts").push({
        "name": caption, "caption": caption,
        "image": image_url, "redirect": video_url,
        "premium": premium, "isNew": True,
        "order": int(time.time() * 1000)
    })
    return post.key

def set_premium(post_id: str, premium: bool):
    db.reference(f"posts/{post_id}").update({"premium": premium})

# ── Upload helpers ────────────────────────────────────────────────────────────
async def upload_to_pixeldrain(file_path: str, filename: str) -> str:
    try:
        async with aiohttp.ClientSession() as s:
            with open(file_path, "rb") as f:
                form = aiohttp.FormData()
                form.add_field("file", f, filename=filename)
                async with s.post(
                    PIXELDRAIN_URL, data=form,
                    auth=aiohttp.BasicAuth("", PIXELDRAIN_KEY),
                    timeout=aiohttp.ClientTimeout(total=600)
                ) as res:
                    data = await res.json()
                    if data.get("id"):
                        return f"https://pixeldrain.com/api/file/{data['id']}"
                    logger.error(f"Pixeldrain failed: {data}")
                    return ""
    except Exception as e:
        logger.error(f"Pixeldrain error: {e}")
        return ""

async def upload_to_catbox(data: bytes, filename: str) -> str:
    try:
        async with aiohttp.ClientSession() as s:
            form = aiohttp.FormData()
            form.add_field("reqtype", "fileupload")
            form.add_field("fileToUpload", data, filename=filename, content_type="image/jpeg")
            async with s.post(CATBOX_URL, data=form, timeout=aiohttp.ClientTimeout(total=60)) as res:
                result = (await res.text()).strip()
                return result if result.startswith("https://") else ""
    except Exception as e:
        logger.error(f"Catbox error: {e}")
        return ""

# ── Core processor ────────────────────────────────────────────────────────────
async def process_task(bot, task: dict):
    global current_video_event, current_video_media

    status_msg   = task["status_msg"]
    image_url    = task["image_url"]
    terabox_link = task["terabox_link"]

    try:
        await status_msg.edit(
            "📨 *Step 1/4:* @TeraBoxDownloader_TgBot ko link bhej raha hoon...",
            parse_mode="markdown"
        )
        await bot.send_message(TERABOX_BOT, terabox_link)

        await status_msg.edit(
            "⏳ *Step 2/4:* Channel mein video ka wait...",
            parse_mode="markdown"
        )
        current_video_event.clear()
        current_video_media = None

        try:
            await asyncio.wait_for(current_video_event.wait(), timeout=300)
        except asyncio.TimeoutError:
            await status_msg.edit("❌ Timeout — 5 min mein video nahi aai. Post skip ho gayi.")
            return

        media = current_video_media
        if not media or not hasattr(media, "document"):
            await status_msg.edit("❌ Video invalid. Skip.")
            return

        doc = media.document
        filename = next((a.file_name for a in doc.attributes if hasattr(a, "file_name")), "video.mp4")
        size_mb  = doc.size / 1024 / 1024

        await status_msg.edit(
            f"📥 *Step 3/4:* Download ho rahi hai...\n📁 `{filename}` ({size_mb:.1f} MB)",
            parse_mode="markdown"
        )

        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
            tmp_path = tmp.name

        last_edit = time.time()
        async def progress_cb(current, total):
            nonlocal last_edit
            if time.time() - last_edit > 5:
                last_edit = time.time()
                pct = int(current / total * 100) if total else 0
                try:
                    await status_msg.edit(f"📥 Download: {pct}% ({current/1024/1024:.1f}/{total/1024/1024:.1f} MB)")
                except: pass

        await bot.download_media(media, tmp_path, progress_callback=progress_cb)

        # Thumbnail — user ki image prefer, fallback: video thumbnail
        if not image_url:
            try:
                thumb = await bot.download_media(media, bytes, thumb=-1)
                if thumb:
                    image_url = await upload_to_catbox(thumb, "thumb.jpg")
            except Exception as e:
                logger.warning(f"Thumbnail failed: {e}")

        await status_msg.edit("☁️ *Step 4/4:* Pixeldrain pe upload ho raha hai...", parse_mode="markdown")
        pd_url = await upload_to_pixeldrain(tmp_path, filename)

        try: os.unlink(tmp_path)
        except: pass

        if not pd_url:
            await status_msg.edit("❌ Pixeldrain upload failed!")
            return

        caption  = random.choice(CAPTIONS)
        loop     = asyncio.get_event_loop()
        post_id  = await loop.run_in_executor(None, save_post, pd_url, image_url, caption, False)
        post_num = await loop.run_in_executor(None, get_post_number, post_id)

        await status_msg.delete()
        await bot.send_message(
            ADMIN_ID,
            f"✅ *Post #{post_num} Complete!*\n\n"
            f"🆔 `{post_id}`\n"
            f"📝 _{caption}_\n"
            f"📁 `{filename}` ({size_mb:.1f} MB)\n"
            f"🎬 {pd_url}\n"
            f"🖼️ {image_url or 'N/A'}\n"
            f"👑 Status: 🆓 Free",
            parse_mode="markdown",
            buttons=[[
                Button.inline("👑 Premium Karo", f"premium:{post_id}".encode()),
                Button.inline("🆓 Free Rakho",  f"free:{post_id}".encode()),
            ]]
        )
        logger.info(f"✅ Post #{post_num} saved: {post_id}")

    except Exception as e:
        logger.error(f"process_task error: {e}")
        try: await status_msg.edit(f"❌ Error: {e}")
        except: pass

# ── Queue worker ──────────────────────────────────────────────────────────────
async def queue_worker(bot):
    global is_processing
    while True:
        task = await task_queue.get()
        is_processing = True
        try:
            await process_task(bot, task)
        except Exception as e:
            logger.error(f"Queue worker error: {e}")
        finally:
            is_processing = False
            task_queue.task_done()
        await asyncio.sleep(2)

# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    global current_video_media

    # Session string se client banao — file system ki zaroorat nahi
    # User session se connect karo (bot dusre bot ko message nahi kar sakta)
    bot = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await bot.start()  # User account se — phone/OTP nahi maangega (session string se)
    logger.info("🚀 PagalBhabhi Bot started!")

    # Queue worker start karo
    asyncio.create_task(queue_worker(bot))

    # ── Handlers ──────────────────────────────────────────────────────────────
    @bot.on(events.NewMessage(pattern="/start"))
    async def start(event):
        if event.sender_id != ADMIN_ID: return
        await event.reply(
            "🔥 *PagalBhabhi Bot*\n\n"
            "📤 Image + Terabox link bhejo\n"
            "👑 `/premium POST_ID`\n"
            "🆓 `/free POST_ID`\n"
            "📊 `/status`",
            parse_mode="markdown"
        )

    @bot.on(events.NewMessage(pattern="/status"))
    async def status(event):
        if event.sender_id != ADMIN_ID: return
        posts   = db.reference("posts").get() or {}
        premium = sum(1 for p in posts.values() if p.get("premium"))
        await event.reply(
            f"📊 *Status*\n\n"
            f"📹 Posts: `{len(posts)}`\n"
            f"👑 Premium: `{premium}`\n"
            f"🆓 Free: `{len(posts)-premium}`\n"
            f"⏳ Queue: `{task_queue.qsize()}` tasks\n"
            f"🔄 Processing: `{'Haan' if is_processing else 'Nahi'}`",
            parse_mode="markdown"
        )

    @bot.on(events.NewMessage(pattern=r"/premium (.+)"))
    async def premium_cmd(event):
        if event.sender_id != ADMIN_ID: return
        post_id = event.pattern_match.group(1).strip()
        try:
            set_premium(post_id, True)
            await event.reply(f"👑 Post #{get_post_number(post_id)} premium ho gaya!")
        except Exception as e:
            await event.reply(f"❌ {e}")

    @bot.on(events.NewMessage(pattern=r"/free (.+)"))
    async def free_cmd(event):
        if event.sender_id != ADMIN_ID: return
        post_id = event.pattern_match.group(1).strip()
        try:
            set_premium(post_id, False)
            await event.reply(f"🆓 Post #{get_post_number(post_id)} free ho gaya!")
        except Exception as e:
            await event.reply(f"❌ {e}")

    @bot.on(events.CallbackQuery())
    async def callback(event):
        if event.sender_id != ADMIN_ID:
            return await event.answer("❌ Sirf admin!", alert=True)
        data = event.data.decode()
        if ":" not in data:
            return await event.answer("Already set hai!", alert=True)
        action, post_id = data.split(":", 1)
        try:
            if action == "premium":
                set_premium(post_id, True)
                num = get_post_number(post_id)
                new_text = event.message.text.replace("👑 Status: 🆓 Free", "👑 Status: 👑 Premium")
                await event.edit(new_text, parse_mode="markdown", buttons=[[
                    Button.inline("✅ Premium", b"noop"),
                    Button.inline("🆓 Free Karo", f"free:{post_id}".encode())
                ]])
                await event.answer(f"👑 Post #{num} premium!")
            elif action == "free":
                set_premium(post_id, False)
                num = get_post_number(post_id)
                new_text = event.message.text.replace("👑 Status: 👑 Premium", "👑 Status: 🆓 Free")
                await event.edit(new_text, parse_mode="markdown", buttons=[[
                    Button.inline("👑 Premium Karo", f"premium:{post_id}".encode()),
                    Button.inline("✅ Free", b"noop")
                ]])
                await event.answer(f"🆓 Post #{num} free!")
            elif action == "noop":
                await event.answer("Already set hai!", alert=True)
        except Exception as e:
            await event.answer(f"❌ {e}", alert=True)

    @bot.on(events.NewMessage(
        incoming=True,
        func=lambda e: e.is_private and any(d in (e.text or "") for d in TERABOX_DOMAINS)
    ))
    async def terabox_handler(event):
        if event.sender_id != ADMIN_ID: return
        text = (event.text or "").strip()

        image_url = ""
        if event.photo:
            try:
                img_bytes = await bot.download_media(event.photo, bytes)
                if img_bytes:
                    image_url = await upload_to_catbox(img_bytes, "thumb.jpg")
            except Exception as e:
                logger.warning(f"Image upload failed: {e}")

        q_size = task_queue.qsize()
        status_msg = await event.reply(
            f"📋 *Queue mein add ho gaya!*\n\n"
            f"🔢 Position: `#{q_size + 1}`\n"
            f"🖼️ Image: {'✅' if image_url else '❌ Nahi mili'}\n"
            f"⏳ Processing shuru hogi jab pehli complete ho...",
            parse_mode="markdown"
        )
        await task_queue.put({
            "status_msg":   status_msg,
            "image_url":    image_url,
            "terabox_link": text
        })

    @bot.on(events.NewMessage(chats=CHANNEL_ID, func=lambda e: e.media))
    async def channel_handler(event):
        global current_video_media
        media = event.media
        if not hasattr(media, "document"): return
        if not (media.document.mime_type or "").startswith("video/"): return
        logger.info("✅ Channel mein video aaya!")
        current_video_media = media
        current_video_event.set()

    # Health check server — Render ke liye port binding zaroori hai
    from aiohttp import web

    async def health(request):
        return web.Response(text="OK")

    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)

    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Health check server started on port {port}")

    await bot.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
