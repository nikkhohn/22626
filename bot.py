import os, json, time, random, asyncio, aiohttp, logging, tempfile
from dotenv import load_dotenv
load_dotenv()

from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
import firebase_admin
from firebase_admin import credentials, db

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
API_ID         = int(os.environ["API_ID"])
API_HASH       = os.environ["API_HASH"]
BOT_TOKEN      = os.environ["BOT_TOKEN"]
ADMIN_ID       = int(os.environ["ADMIN_ID"])
CHANNEL_ID     = int(os.environ["CHANNEL_ID"])
FIREBASE_URL   = os.environ["FIREBASE_URL"]
PIXELDRAIN_KEY = os.environ["PIXELDRAIN_API_KEY"]
SESSION_STRING = os.environ["SESSION_STRING"]
TERABOX_BOT    = os.environ.get("TERABOX_BOT", "@TeraBoxDownloader_TgBot")
FIREBASE_CRED_JSON = os.environ.get("FIREBASE_CRED_JSON", "")
FIREBASE_CRED_PATH = os.environ.get("FIREBASE_CRED_PATH", "serviceAccountKey.json")

TERABOX_DOMAINS = ["terabox.com","1024terabox.com","terafileshare.com","terasharelink.com","teraboxapp.com","terabox.app"]

CAPTIONS = [
    "Ekdum Mast Content! Dekhte raho...","Aaj ka sabse hot upload!",
    "Itna spicy content pehle kabhi nahi dekha!","Dhamaka content! Miss mat karna...",
    "Premium quality, free mein enjoy karo!","Ye dekh ke pagal ho jaoge!",
    "Sirf adults ke liye — 18+ content!","Aaj raat ke liye perfect entertainment!",
    "Full masti, full entertainment!","Popcorn lo aur enjoy karo!",
    "Bhabhi ka naya jawab nahi!","Devar bhabhi ka dhamakedar scene!",
    "Aaj ki raat rangeen hogi!","Ye video dekhe bina mat sona!",
    "Seedha dil pe lagega yeh content!",
]

# ── Firebase ──────────────────────────────────────────────────────────────────
cred = credentials.Certificate(json.loads(FIREBASE_CRED_JSON) if FIREBASE_CRED_JSON else FIREBASE_CRED_PATH)
firebase_admin.initialize_app(cred, {"databaseURL": FIREBASE_URL})

# ── State ─────────────────────────────────────────────────────────────────────
task_queue     = asyncio.Queue()
is_processing  = False
video_event    = asyncio.Event()
video_media    = None

# ── Firebase helpers ──────────────────────────────────────────────────────────
def save_post(video_url, image_url, caption):
    post = db.reference("posts").push({
        "name": caption, "caption": caption,
        "image": image_url, "redirect": video_url,
        "premium": False, "isNew": True,
        "order": int(time.time() * 1000)
    })
    return post.key

def get_post_num(post_id):
    try:
        posts = db.reference("posts").get() or {}
        ids = sorted(posts, key=lambda k: posts[k].get("order",0), reverse=True)
        return ids.index(post_id)+1 if post_id in ids else 0
    except: return 0

def set_premium(post_id, val):
    db.reference(f"posts/{post_id}").update({"premium": val})

# ── Upload helpers ────────────────────────────────────────────────────────────
async def catbox_upload(data: bytes, filename: str) -> str:
    try:
        async with aiohttp.ClientSession() as s:
            form = aiohttp.FormData()
            form.add_field("reqtype", "fileupload")
            form.add_field("fileToUpload", data, filename=filename, content_type="image/jpeg")
            async with s.post("https://catbox.moe/user/api.php", data=form, timeout=aiohttp.ClientTimeout(total=60)) as r:
                res = (await r.text()).strip()
                return res if res.startswith("https://") else ""
    except Exception as e:
        logger.error(f"Catbox error: {e}"); return ""

async def pixeldrain_upload(path: str, filename: str) -> str:
    try:
        async with aiohttp.ClientSession() as s:
            with open(path, "rb") as f:
                form = aiohttp.FormData()
                form.add_field("file", f, filename=filename)
                async with s.post(
                    "https://pixeldrain.com/api/file", data=form,
                    auth=aiohttp.BasicAuth("", PIXELDRAIN_KEY),
                    timeout=aiohttp.ClientTimeout(total=600)
                ) as r:
                    data = await r.json()
                    if data.get("id"):
                        return f"https://pixeldrain.com/api/file/{data['id']}"
                    logger.error(f"Pixeldrain: {data}"); return ""
    except Exception as e:
        logger.error(f"Pixeldrain error: {e}"); return ""

# ── Process one task ──────────────────────────────────────────────────────────
async def process(user_client, bot_client, task):
    global video_event, video_media
    status  = task["status"]
    img_url = task["image_url"]
    link    = task["link"]

    try:
        # 1. TeraBox bot ko link bhejo (user account se)
        await status.edit("📨 TeraBox bot ko link bhej raha hoon...")
        await user_client.send_message(TERABOX_BOT, link)

        # 2. Channel mein video ka wait
        await status.edit("⏳ Channel mein video ka wait kar raha hoon... (max 5 min)")
        video_event.clear()
        video_media = None
        try:
            await asyncio.wait_for(video_event.wait(), timeout=300)
        except asyncio.TimeoutError:
            await status.edit("❌ Timeout — 5 min mein video nahi aai. Post skip.")
            return

        media = video_media
        if not media or not hasattr(media, "document"):
            await status.edit("❌ Video nahi mili. Skip.")
            return

        doc      = media.document
        filename = next((a.file_name for a in doc.attributes if hasattr(a,"file_name")), "video.mp4")
        size_mb  = doc.size / 1024 / 1024

        # 3. Video download
        await status.edit(f"📥 Download ho rahi hai... {size_mb:.1f} MB")
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
            tmp_path = tmp.name

        last = time.time()
        async def prog(cur, tot):
            nonlocal last
            if time.time()-last > 5:
                last = time.time()
                pct = int(cur/tot*100) if tot else 0
                try: await status.edit(f"📥 Download: {pct}% ({cur/1024/1024:.1f}/{tot/1024/1024:.1f} MB)")
                except: pass

        await user_client.download_media(media, tmp_path, progress_callback=prog)

        # 4. Thumbnail (user image prefer, fallback video thumb)
        if not img_url:
            try:
                thumb = await user_client.download_media(media, bytes, thumb=-1)
                if thumb: img_url = await catbox_upload(thumb, "thumb.jpg")
            except: pass

        # 5. Pixeldrain upload
        await status.edit("☁️ Pixeldrain pe upload ho raha hai...")
        pd_url = await pixeldrain_upload(tmp_path, filename)
        try: os.unlink(tmp_path)
        except: pass

        if not pd_url:
            await status.edit("❌ Pixeldrain upload failed!")
            return

        # 6. Firebase save
        caption  = random.choice(CAPTIONS)
        loop     = asyncio.get_event_loop()
        post_id  = await loop.run_in_executor(None, save_post, pd_url, img_url, caption)
        post_num = await loop.run_in_executor(None, get_post_num, post_id)

        await status.delete()
        await bot_client.send_message(
            ADMIN_ID,
            f"✅ *Post #{post_num} Complete!*\n\n"
            f"🆔 `{post_id}`\n"
            f"📝 _{caption}_\n"
            f"📁 `{filename}` ({size_mb:.1f} MB)\n"
            f"🎬 {pd_url}\n"
            f"🖼️ {img_url or 'N/A'}\n"
            f"👑 Status: 🆓 Free",
            parse_mode="markdown",
            buttons=[[
                Button.inline("👑 Premium Karo", f"premium:{post_id}".encode()),
                Button.inline("🆓 Free Rakho",  f"free:{post_id}".encode()),
            ]]
        )
        logger.info(f"✅ Post #{post_num}: {post_id}")

    except Exception as e:
        logger.error(f"process error: {e}")
        try: await status.edit(f"❌ Error: {e}")
        except: pass

# ── Queue worker ──────────────────────────────────────────────────────────────
async def worker(user_client, bot_client):
    global is_processing
    while True:
        task = await task_queue.get()
        is_processing = True
        try: await process(user_client, bot_client, task)
        except Exception as e: logger.error(f"worker error: {e}")
        finally:
            is_processing = False
            task_queue.task_done()
        await asyncio.sleep(2)

# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    global video_media

    # User client — TeraBox bot ko message karne ke liye
    user_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await user_client.start()
    logger.info("✅ User client connected!")

    # Bot client — Admin ko messages bhejne ke liye
    bot_client = TelegramClient("bot_session", API_ID, API_HASH)
    await bot_client.start(bot_token=BOT_TOKEN)
    logger.info("✅ Bot client connected!")

    asyncio.create_task(worker(user_client, bot_client))

    # ── User client handlers ───────────────────────────────────────────────────

    @user_client.on(events.NewMessage(
        incoming=True,
        func=lambda e: e.is_private and e.sender_id == ADMIN_ID
                       and any(d in (e.text or "") for d in TERABOX_DOMAINS)
    ))
    async def on_post(event):
        text = (event.text or "").strip()

        # Image upload to catbox
        img_url = ""
        if event.photo:
            try:
                img_bytes = await user_client.download_media(event.photo, bytes)
                if img_bytes:
                    img_url = await catbox_upload(img_bytes, "thumb.jpg")
                    logger.info(f"Image catbox: {img_url}")
            except Exception as e:
                logger.warning(f"Image upload failed: {e}")

        q = task_queue.qsize()
        status = await event.reply(
            f"📋 Queue #{q+1} mein add!\n"
            f"🖼️ Image: {'✅' if img_url else '❌'}\n"
            f"⏳ Processing...",
        )
        await task_queue.put({"status": status, "image_url": img_url, "link": text})
        logger.info(f"Task queued. Size: {task_queue.qsize()}")

    @user_client.on(events.NewMessage(chats=CHANNEL_ID, func=lambda e: e.media))
    async def on_channel_video(event):
        global video_media
        media = event.media
        if not hasattr(media, "document"): return
        if not (media.document.mime_type or "").startswith("video/"): return
        logger.info("✅ Channel mein video aaya!")
        video_media = media
        video_event.set()

    # ── Bot client handlers ────────────────────────────────────────────────────

    @bot_client.on(events.NewMessage(pattern="/start"))
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

    @bot_client.on(events.NewMessage(pattern="/status"))
    async def status_cmd(event):
        if event.sender_id != ADMIN_ID: return
        posts = db.reference("posts").get() or {}
        prem  = sum(1 for p in posts.values() if p.get("premium"))
        await event.reply(
            f"📊 *Status*\n\n"
            f"📹 Posts: `{len(posts)}`\n"
            f"👑 Premium: `{prem}`\n"
            f"🆓 Free: `{len(posts)-prem}`\n"
            f"⏳ Queue: `{task_queue.qsize()}`\n"
            f"🔄 Processing: `{'Haan' if is_processing else 'Nahi'}`",
            parse_mode="markdown"
        )

    @bot_client.on(events.NewMessage(pattern=r"/premium (.+)"))
    async def premium_cmd(event):
        if event.sender_id != ADMIN_ID: return
        pid = event.pattern_match.group(1).strip()
        try:
            set_premium(pid, True)
            await event.reply(f"👑 Post #{get_post_num(pid)} premium!")
        except Exception as e: await event.reply(f"❌ {e}")

    @bot_client.on(events.NewMessage(pattern=r"/free (.+)"))
    async def free_cmd(event):
        if event.sender_id != ADMIN_ID: return
        pid = event.pattern_match.group(1).strip()
        try:
            set_premium(pid, False)
            await event.reply(f"🆓 Post #{get_post_num(pid)} free!")
        except Exception as e: await event.reply(f"❌ {e}")

    @bot_client.on(events.CallbackQuery())
    async def callback(event):
        if event.sender_id != ADMIN_ID:
            return await event.answer("❌ Sirf admin!", alert=True)
        data = event.data.decode()
        if ":" not in data:
            return await event.answer("Already set hai!", alert=True)
        action, pid = data.split(":", 1)
        try:
            if action == "premium":
                set_premium(pid, True)
                new_text = event.message.text.replace("🆓 Free", "👑 Premium")
                await event.edit(new_text, parse_mode="markdown", buttons=[[
                    Button.inline("✅ Premium", b"noop"),
                    Button.inline("🆓 Free Karo", f"free:{pid}".encode())
                ]])
                await event.answer(f"👑 Post #{get_post_num(pid)} premium!")
            elif action == "free":
                set_premium(pid, False)
                new_text = event.message.text.replace("👑 Premium", "🆓 Free")
                await event.edit(new_text, parse_mode="markdown", buttons=[[
                    Button.inline("👑 Premium Karo", f"premium:{pid}".encode()),
                    Button.inline("✅ Free", b"noop")
                ]])
                await event.answer(f"🆓 Post #{get_post_num(pid)} free!")
            elif action == "noop":
                await event.answer("Already set hai!", alert=True)
        except Exception as e:
            await event.answer(f"❌ {e}", alert=True)

    # Health check for Render
    from aiohttp import web
    async def health(r): return web.Response(text="OK")
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8000))
    await web.TCPSite(runner, "0.0.0.0", port).start()
    logger.info(f"🚀 Bot started! Port: {port}")

    await asyncio.gather(
        user_client.run_until_disconnected(),
        bot_client.run_until_disconnected()
    )

if __name__ == "__main__":
    asyncio.run(main())
