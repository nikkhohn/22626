import os
import re
import time
import random
import asyncio
import aiohttp
import logging
import tempfile
from dotenv import load_dotenv

load_dotenv()

from telethon import TelegramClient, events, Button
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
CHANNEL_ID     = int(os.environ["CHANNEL_ID"])       # jahan @TeraBoxDownloader_TgBot video bhejta hai
FIREBASE_CRED  = os.environ["FIREBASE_CRED_PATH"]
FIREBASE_URL   = os.environ["FIREBASE_URL"]
PIXELDRAIN_KEY = os.environ["PIXELDRAIN_API_KEY"]
TERABOX_BOT    = os.environ.get("TERABOX_BOT", "@TeraBoxDownloader_TgBot")

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
cred = credentials.Certificate(FIREBASE_CRED)
firebase_admin.initialize_app(cred, {"databaseURL": FIREBASE_URL})

# ── Pending tasks ─────────────────────────────────────────────────────────────
# key: float timestamp, value: {"status_msg", "image_url"}
pending: dict = {}

# ── Bot client ────────────────────────────────────────────────────────────────
bot = TelegramClient("pagalbhabhi", API_ID, API_HASH).start(bot_token=BOT_TOKEN)

# ── Firebase helpers ──────────────────────────────────────────────────────────
def get_post_number(post_id: str) -> int:
    try:
        posts = db.reference("posts").get() or {}
        sorted_ids = sorted(posts.keys(), key=lambda k: posts[k].get("order", 0), reverse=True)
        return sorted_ids.index(post_id) + 1 if post_id in sorted_ids else 0
    except:
        return 0

def save_post(video_url: str, image_url: str, caption: str, premium: bool = False) -> str:
    ref = db.reference("posts")
    post = ref.push({
        "name": caption,
        "caption": caption,
        "image": image_url,
        "redirect": video_url,
        "premium": premium,
        "isNew": True,
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
                    PIXELDRAIN_URL,
                    data=form,
                    auth=aiohttp.BasicAuth("", PIXELDRAIN_KEY),
                    timeout=aiohttp.ClientTimeout(total=600)
                ) as res:
                    data = await res.json()
                    if data.get("id"):
                        fid = data["id"]
                        logger.info(f"Pixeldrain OK: {fid}")
                        return f"https://pixeldrain.com/api/file/{fid}"
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
            async with s.post(
                CATBOX_URL,
                data=form,
                timeout=aiohttp.ClientTimeout(total=60)
            ) as res:
                result = (await res.text()).strip()
                if result.startswith("https://"):
                    logger.info(f"Catbox OK: {result}")
                    return result
                logger.error(f"Catbox failed: {result}")
                return ""
    except Exception as e:
        logger.error(f"Catbox error: {e}")
        return ""

# ── Handlers ──────────────────────────────────────────────────────────────────

@bot.on(events.NewMessage(pattern="/start"))
async def start(event):
    if event.sender_id != ADMIN_ID:
        return
    await event.reply(
        "🔥 *PagalBhabhi Bot*\n\n"
        "📤 Image + Terabox link ek saath bhejo\n"
        "👑 `/premium POST_ID` — premium karo\n"
        "🆓 `/free POST_ID` — free karo\n"
        "📊 `/status` — stats",
        parse_mode="markdown"
    )

@bot.on(events.NewMessage(pattern="/status"))
async def status(event):
    if event.sender_id != ADMIN_ID:
        return
    posts = db.reference("posts").get() or {}
    premium = sum(1 for p in posts.values() if p.get("premium"))
    await event.reply(
        f"📊 *Status*\n\n"
        f"📹 Posts: `{len(posts)}`\n"
        f"👑 Premium: `{premium}`\n"
        f"🆓 Free: `{len(posts) - premium}`\n"
        f"⏳ Pending: `{len(pending)}`",
        parse_mode="markdown"
    )

@bot.on(events.NewMessage(pattern=r"/premium (.+)"))
async def premium_cmd(event):
    if event.sender_id != ADMIN_ID:
        return
    post_id = event.pattern_match.group(1).strip()
    try:
        set_premium(post_id, True)
        num = get_post_number(post_id)
        await event.reply(f"👑 Post #{num} premium ho gaya!")
    except Exception as e:
        await event.reply(f"❌ {e}")

@bot.on(events.NewMessage(pattern=r"/free (.+)"))
async def free_cmd(event):
    if event.sender_id != ADMIN_ID:
        return
    post_id = event.pattern_match.group(1).strip()
    try:
        set_premium(post_id, False)
        num = get_post_number(post_id)
        await event.reply(f"🆓 Post #{num} free ho gaya!")
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
            await event.edit(
                new_text,
                parse_mode="markdown",
                buttons=[
                    [Button.inline("✅ Premium", b"noop"),
                     Button.inline("🆓 Free Karo", f"free:{post_id}".encode())]
                ]
            )
            await event.answer(f"👑 Post #{num} premium ho gaya!")

        elif action == "free":
            set_premium(post_id, False)
            num = get_post_number(post_id)
            new_text = event.message.text.replace("👑 Status: 👑 Premium", "👑 Status: 🆓 Free")
            await event.edit(
                new_text,
                parse_mode="markdown",
                buttons=[
                    [Button.inline("👑 Premium Karo", f"premium:{post_id}".encode()),
                     Button.inline("✅ Free", b"noop")]
                ]
            )
            await event.answer(f"🆓 Post #{num} free ho gaya!")

        elif action == "noop":
            await event.answer("Already set hai!", alert=True)

    except Exception as e:
        await event.answer(f"❌ {e}", alert=True)

@bot.on(events.NewMessage(
    incoming=True,
    func=lambda e: (
        e.is_private
        and e.sender_id
        and (e.photo or e.text)
        and any(d in (e.text or "") for d in TERABOX_DOMAINS)
    )
))
async def terabox_handler(event):
    """
    Admin ne image + Terabox link ek message mein bheja.
    Ya sirf Terabox link bheja (bina image ke).
    """
    if event.sender_id != ADMIN_ID:
        return

    text = (event.text or "").strip()
    status_msg = await event.reply("⏳ Processing...")

    try:
        # Step 1: Image Catbox pe upload karo (agar saath mein hai)
        image_url = ""
        if event.photo:
            await status_msg.edit("🖼️ Image Catbox pe upload ho rahi hai...")
            try:
                img_bytes = await bot.download_media(event.photo, bytes)
                if img_bytes:
                    image_url = await upload_to_catbox(img_bytes, "thumb.jpg")
                    if image_url:
                        logger.info(f"Image Catbox pe upload hua: {image_url}")
                    else:
                        logger.warning("Catbox upload failed — blank thumbnail rahega")
            except Exception as e:
                logger.warning(f"Image upload error: {e}")

        # Step 2: TeraBox bot ko link bhejo
        await status_msg.edit("📨 @TeraBoxDownloader_TgBot ko link bhej raha hoon...")
        await bot.send_message(TERABOX_BOT, text)

        # Step 3: Pending mein save karo
        key = time.time()
        pending[key] = {
            "status_msg": status_msg,
            "image_url": image_url,
            "timestamp": key
        }

        await status_msg.edit(
            "✅ *Link bhej diya!*\n\n"
            f"🖼️ Image: {'✅ Ready' if image_url else '❌ Nahi mili'}\n"
            "⏳ Channel mein video ka wait kar raha hoon...\n"
            "_(1-3 minute lag sakte hain)_",
            parse_mode="markdown"
        )

        # 5 min timeout
        await asyncio.sleep(300)
        if key in pending:
            del pending[key]
            await status_msg.edit(
                "❌ Timeout — TeraBox bot ne 5 min mein video nahi bheja.\n"
                "Dobara try karo."
            )

    except Exception as e:
        logger.error(f"Terabox handler error: {e}")
        await status_msg.edit(f"❌ Error: {e}")

@bot.on(events.NewMessage(chats=CHANNEL_ID, func=lambda e: e.media))
async def channel_video_handler(event):
    """
    Channel mein @TeraBoxDownloader_TgBot ne video bheja.
    Download → Pixeldrain upload → Catbox thumbnail → Firebase save.
    """
    if not pending:
        logger.info("Channel mein media aaya lekin koi pending task nahi")
        return

    media = event.media
    # Sirf video documents handle karo
    if not hasattr(media, "document"):
        return
    doc = media.document
    if not (doc.mime_type or "").startswith("video/"):
        return

    # Oldest pending task lo (FIFO)
    oldest_key = min(pending.keys())
    task = pending.pop(oldest_key)
    status_msg = task["status_msg"]
    image_url = task.get("image_url", "")

    filename = next(
        (a.file_name for a in doc.attributes if hasattr(a, "file_name")),
        "video.mp4"
    )
    size_mb = doc.size / 1024 / 1024

    try:
        await status_msg.edit(
            f"📥 Video mili! Download ho rahi hai...\n"
            f"📁 `{filename}` ({size_mb:.1f} MB)",
            parse_mode="markdown"
        )

        # Video temp file mein download karo
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
            tmp_path = tmp.name

        last_edit = time.time()

        async def progress_cb(current, total):
            nonlocal last_edit
            if time.time() - last_edit > 5:
                last_edit = time.time()
                pct = int(current / total * 100) if total else 0
                try:
                    await status_msg.edit(
                        f"📥 Download: {pct}% "
                        f"({current/1024/1024:.1f}/{total/1024/1024:.1f} MB)"
                    )
                except:
                    pass

        await bot.download_media(event.media, tmp_path, progress_callback=progress_cb)
        logger.info(f"Download complete: {tmp_path} ({size_mb:.1f} MB)")

        # Thumbnail — user ki image prefer karo, fallback: video thumbnail
        if not image_url:
            await status_msg.edit("🖼️ Video thumbnail Catbox pe upload ho rahi hai...")
            try:
                thumb_bytes = await bot.download_media(event.media, bytes, thumb=-1)
                if thumb_bytes:
                    image_url = await upload_to_catbox(thumb_bytes, "thumb.jpg")
                    logger.info(f"Video thumb Catbox: {image_url}")
            except Exception as e:
                logger.warning(f"Video thumbnail failed: {e}")
        else:
            logger.info(f"User ki image use ho rahi hai: {image_url}")

        # Pixeldrain upload
        await status_msg.edit("☁️ Pixeldrain pe upload ho raha hai...")
        pd_url = await upload_to_pixeldrain(tmp_path, filename)

        # Temp file delete
        try:
            os.unlink(tmp_path)
        except:
            pass

        if not pd_url:
            await status_msg.edit(
                "❌ Pixeldrain upload failed!\n"
                "Pixeldrain API key check karo ya baad mein try karo."
            )
            return

        # Firebase save
        await status_msg.edit("💾 Firebase mein save ho raha hai...")
        caption = random.choice(CAPTIONS)
        loop = asyncio.get_event_loop()
        post_id = await loop.run_in_executor(
            None, save_post, pd_url, image_url, caption, False
        )
        post_num = await loop.run_in_executor(None, get_post_number, post_id)

        # Admin ko preview
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
            buttons=[
                [
                    Button.inline("👑 Premium Karo", f"premium:{post_id}".encode()),
                    Button.inline("🆓 Free Rakho", f"free:{post_id}".encode()),
                ]
            ]
        )
        logger.info(f"✅ Post #{post_num} saved: {post_id}")

    except Exception as e:
        logger.error(f"Channel handler error: {e}")
        try:
            await status_msg.edit(f"❌ Error: {e}")
        except:
            pass

# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    logger.info("🚀 PagalBhabhi Bot starting...")
    await bot.run_until_disconnected()

if __name__ == "__main__":
    with bot:
        bot.loop.run_until_complete(main())
