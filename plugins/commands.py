import os
import logging
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from info import START_MSG, CHANNELS, ADMINS, CHANNEL_LINK
from utils import Media

link = 't.me/' + CHANNEL_LINK
logger = logging.getLogger(__name__)


@Client.on_message(filters.command('start'))
async def start(bot, message):
   """Start command handler"""
    buttons = [[
        InlineKeyboardButton('Cari disini', switch_inline_query_current_chat=''),
        InlineKeyboardButton('Inline mode', switch_inline_query=''),
    ],[
        InlineKeyboardButton('Join Channel' url=link),
    ]]
    reply_markup = InlineKeyboardMarkup(buttons)
    await message.reply(START_MSG, reply_markup=reply_markup)


@Client.on_message(filters.command('channel') & filters.user(ADMINS))
async def channel_info(bot, message):
    """Send basic information of channel"""
    if isinstance(CHANNELS, (int, str)):
        channels = [CHANNELS]
    elif isinstance(CHANNELS, list):
        channels = CHANNELS
    else:
        raise ValueError("Unexpected type of CHANNELS")

    for channel in channels:
        channel_info = await bot.get_chat(channel)
        string = str(channel_info)
        if len(string) > 4096:
            filename = (channel_info.title or channel_info.first_name) + ".txt"
            with open(filename, 'w') as f:
                f.write(string)
            await message.reply_document(filename)
            os.remove(filename)
        else:
            await message.reply(str(channel_info))


@Client.on_message(filters.command('total'))
async def total(bot, message):
    """Show total files in database"""
    msg = await message.reply("Lagi di Prosess...⏳", quote=True)
    try:
        total = await Media.count_documents()
        await msg.edit(f'📁 File Tersimpan: {total}')
    except Exception as e:
        logger.exception('Gagal cek total file')
        await msg.edit(f'Error: {e}')


@Client.on_message(filters.command('logger') & filters.user(ADMINS))
async def log_file(bot, message):
    """Send log file"""
    try:
        await message.reply_document('TelegramBot.log')
    except Exception as e:
        await message.reply(str(e))


@Client.on_message(filters.command('hapus') & filters.user(ADMINS))
async def hapus(bot, message):
    """hapus file from database"""
    reply = message.reply_to_message
    if reply and reply.media:
        msg = await message.reply("Lagi di Prosess...⏳", quote=True)
    else:
        await message.reply('Balas file dengan /hapus yang ingin Anda hapus', quote=True)
        return

    for file_type in ("document", "video", "audio"):
        media = getattr(reply, file_type, None)
        if media is not None:
            break
    else:
        await msg.edit('Ini bukanlah format yang Di izinkan')
        return

    result = await Media.collection.hapus_one({
        'file_name': media.file_name,
        'file_size': media.file_size,
        'mime_type': media.mime_type,
        'caption': reply.caption
    })
    if result.hapusd_count:
        await msg.edit('File sukses di hapus dari database')
    else:
        await msg.edit('File tidak ditemukan di database:(')
