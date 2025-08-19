import os
import sqlite3
import re
from telethon import TelegramClient, events
from telethon.tl.types import MessageEntityTextUrl, MessageEntityCustomEmoji
from telethon.errors import MessageNotModifiedError, ChatAdminRequiredError
import asyncio
from datetime import datetime, timedelta
import pytz
from config import config

api_id = config.API_ID
api_hash = config.API_HASH
phone_number = config.PHONE_NUMBER

pinned_monitoring_chats = [-1002844850017, -1002880304508]

EMOJI_REPLACEMENTS = {
    5372880993533323367: 5206607235953752634,
    5373063542528307348: 5206654587968190247,
    5280785727792108770: 5206719205751161131,
    5280922805968328796: 5204115338878287650,
    5247244627370600418: 5204079484491299211,
    5197393264088474474: 5206335420358491796,
    5190597311566208526: 5206330597110219293,
    5408841371723263170: 5206407193556973192,
    5251353867395820981: 5206223794158467533,
    5244476495178525952: 5206466541415074273,
    5247058354638973537: 5206255701470510088,
    5371010672714866594: 5206415122066602104,
    5251282081312435149: 5206304758586963022,
    5204021824555345338: 5204407194790951395,
    5204081653449779192: 5204068270331689043,
    5289582903146082226: 5206191401515121501,
    5373035745499965443: 5204194516600388754,
    5283278058659275177: 5204328236112182989,
    5305537237771903979: 5204313869446574619,
    5238064542797291332: 5204253456436589423,
    5253640104257284843: 5204454795913505458,
    5314542379737316964: 5204163034490109835,
    5253854491844831055: 5204153284914346208,
    5246833276877825195: 5204005151492309045,
    5246759824347128107: 5204260083571129354,
    5400292308040047721: 5206719334600176779,
    5402603039099997690: 5204313216611547462,
    5373049450740607197: 5203969803911462878,
    5370719362263049137: 5206532275889535527,
    5215369020777196160: 5206355976071974068,
    5474349248008971212: 5206467649516634461,
    5445024194859393115: 5203965126692075777,
    5444886747315990720: 5206198015764758510,
    5247028246918225636: 5204436941734443348,
    5447107820933637815: 5206600016113728102,
    5328157396000857690: 5203923233581071324,
    5208693812670524323: 5206701768183938632,
    5465199494494708621: 5204291436832390758,
    5463081186559550207: 5204118491384285773,
    5195214878150779865: 5204309531529607790,
    5194902900316333383: 5204077547461048610,
    5375505497724054116: 5206636759558946232,
    5465273264852985632: 5204388932590009926,
    5384376885817927331: 5204402710845093850,
    5373084798321452785: 5204355479089741113,
    5307585683769027807: 5206517990828309182,
    5373080821181737452: 5206652960175587424,
    5460664167943922246: 5204332316331111959,
    5444943329215145417: 5206181969766941003,
    5291876402797241283: 5206442446648540059,
    5247193014748603222: 5206364471517283309,
    5372917015424039198: 5204272019285245215,
    5375597508808434576: 5206670625376074763,
    5375330078374781606: 5204012225303445676,
    5400268939122986915: 5206228570162100284,
    5447399908774537012: 5206512390190956210,
    5462997224243879057: 5206705418906142773,
    5269644531086675634: 5203988057522469965,
    5467492195281955589: 5206630080884798429,
    5221959592258332525: 5206689454512701400,
    5188421433889420079: 5206185487345154600,
    5384201926030157244: 5203979394573433028,
    5280978803751931759: 5204158760997649018,
    5280911767902378209: 5204253838688680876,
    5188606001519027917: 5206708730325924274,
    5204280175428131884: 5204300795566127631,
    5465665167733838930: 5206656314545045023,
    5280890726857594571: 5203971985754847024,
    5280769802053375986: 5206719463449200087,
    5330572142578782411: 5204252734882083691,
    5444998983401371287: 5206633920585564594,
    5454315076704024352: 5197313205898083478,
    5456489305113377267: 5194948220811245371,
    5456247464094873111: 5197234449082776943,
    5458714433180152890: 5197655965763139532,
    5458391082272306916: 5197666900749876487,
    5461139985895798124: 5195049620694138265,
    5460719628856605061: 5195052975063594542,
    5460733780773846489: 5195377451252872108,
    5460814298525743657: 5197348987270624669,
    5190598496977171599: 5197352302985379039,
    5303031304743303175: 5195122841296601045,
    5303320978812580745: 5195286750133515852,
    5335035390988401429: 5195252407575018375,
    5422558028387862398: 5197237640243479448,
    5425081351739035131: 5197641616277405183,
    5422536167004334104: 5195419086665841429,
    5422366752019336275: 5194998489108479820,
    5429444888187914525: 5197431338973559198,
    5445153550684407809: 5195440819200359295,
    5202130660260598974: 5197482204271244922,
    5334757283266057489: 5197488921600097063,
    5393505632647139005: 5195120457589749804,
    5235730223776949445: 5197209083005924401,
    5197643394393851318: 5197313205898083478,
    5818665600624365278: 5206719205751161131,
    5409302719930318221: 5206467649516634461,
    5431760098898757915: 5206467649516634461,
    5447228621183807807: 5206633920585564594,
    "⚪": 5197491966731912094,
    "⚫": 5294124676442768019,
    "🚀": 5195419086665841429,
    "💣": 5195052975063594542,
}

blacklist_emojis = ["🥇", "🥈", "🥉"]
blacklist_words = ["Ринда", "збор", "каналу", "збір", "друзі", "грн", "Підтримайте", "слухає"]

unrestricted_channel_id = -1002882017852
deletion_command_channel_id = -1002826123958
source_channels = [unrestricted_channel_id, -1001875486764, -1002668349125]
target_channel_ids = [-1002606152715, -1002512234056]
monitored_channel_id = -1001875486764

keywords = [
    "БпЛа", "БпЛА", "БпЛA", "курсом", "Каб", "Каби", "Шахед", "Shahed",
    "А-50У", "борту", "Не фіксується", "ціль", "вибух", "ппо", "ППО",
    "укриття", "новий", "ще", "обережно", "загиблі", "жертви", "рух",
    "Дорозвідка", "тривог", "тривога", "обережно", "уважно", "ракета",
    "ракети", "КР", "Загроза", "балістичного", "Зліт", "Ту-", "Один",
    "Два", "Три", "п'ять", "сімь", "МіГ-", "аеродромx", "здійснили посадку",
    "мінус", "ціль", "цілі", "бік", "рф", "Зафіксовано пуски ударних БпЛА", "Загроза застосування", "Відбій ракетної", "Балістика", "Zala", "Орлан-10"
]

client = TelegramClient('monik3', api_id, api_hash)
scheduled_deletions = {}
pending_del_commands = {}

KYIV_TZ = pytz.timezone('Europe/Kiev')

CHANNEL_NAMES = {
    -1002512234056: "ГОЛОВНА | ПАТРУЛЬ УКРАЇНИ🇺🇦",
    -1002826123958: "Патруль України | 🇺🇦 | Радар Небезпеки | Куди Летить?",
}

def get_channel_name(channel_id):
    return CHANNEL_NAMES.get(channel_id, str(channel_id))

def log_rejection(reason, message):
    print(f"🚫 Повідомлення {message.id} відхилено: {reason}")
    if message.text:
        if "заборонені слова" in reason:
            found_words = [word for word in blacklist_words if word.lower() in message.text.lower()]
            print(f"🔞 Знайдено заборонені слова: {', '.join(found_words)}")
        elif "заборонені емодзі" in reason:
            found_emojis = [emoji for emoji in blacklist_emojis if emoji in message.text]
            print(f"🔞 Знайдено заборонені емодзі: {', '.join(found_emojis)}")
        elif "цифрові послідовності" in reason:
            numbers = re.findall(r'(?:\d[\s]*){12,}', message.text)
            print(f"🔞 Знайдено заборонені цифрові послідовності: {', '.join(numbers)}")

def init_db():
    conn = sqlite3.connect('messages.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS message_links (
                        original_message_id INTEGER,
                        source_channel_id INTEGER,
                        target_channel_id INTEGER,
                        channel_message_id INTEGER,
                        PRIMARY KEY (original_message_id, source_channel_id, target_channel_id))''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS scheduled_deletions (
                        message_id INTEGER,
                        chat_id INTEGER,
                        delete_at TEXT,
                        log_message_id INTEGER,
                        original_del_command_id INTEGER,
                        PRIMARY KEY (message_id, chat_id))''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS channel_links (
                        link TEXT PRIMARY KEY,
                        message_id INTEGER,
                        chat_id INTEGER)''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS pinned_messages (
                        chat_id INTEGER,
                        message_id INTEGER,
                        processed INTEGER DEFAULT 0,
                        PRIMARY KEY (chat_id, message_id))''')
    conn.commit()
    conn.close()

def clear_pinned_messages_db(chat_id):
    conn = sqlite3.connect('messages.db')
    cursor = conn.cursor()
    cursor.execute('DELETE FROM pinned_messages WHERE chat_id = ?', (chat_id,))
    conn.commit()
    conn.close()
    print(f"🧹 Очищено записи про оброблені повідомлення для чату {chat_id}")

def save_message_link(original_message_id, source_channel_id, target_channel_id, channel_message_id):
    conn = sqlite3.connect('messages.db')
    cursor = conn.cursor()
    cursor.execute('INSERT OR REPLACE INTO message_links VALUES (?, ?, ?, ?)',
                   (original_message_id, source_channel_id, target_channel_id, channel_message_id))
    conn.commit()
    conn.close()

def get_channel_message_id(original_message_id, source_channel_id, target_channel_id):
    conn = sqlite3.connect('messages.db')
    cursor = conn.cursor()
    cursor.execute('SELECT channel_message_id FROM message_links WHERE original_message_id = ? AND source_channel_id = ? AND target_channel_id = ?', 
                   (original_message_id, source_channel_id, target_channel_id))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

def save_scheduled_deletion(message_id, chat_id, delete_at, log_message_id=None, original_del_command_id=None):
    conn = sqlite3.connect('messages.db')
    cursor = conn.cursor()
    cursor.execute('INSERT OR REPLACE INTO scheduled_deletions VALUES (?, ?, ?, ?, ?)',
                   (message_id, chat_id, delete_at, log_message_id, original_del_command_id))
    conn.commit()
    conn.close()

def get_scheduled_deletions():
    conn = sqlite3.connect('messages.db')
    cursor = conn.cursor()
    cursor.execute('SELECT message_id, chat_id, delete_at, log_message_id, original_del_command_id FROM scheduled_deletions')
    result = cursor.fetchall()
    conn.close()
    return result

def delete_scheduled_deletion(message_id, chat_id):
    conn = sqlite3.connect('messages.db')
    cursor = conn.cursor()
    cursor.execute('DELETE FROM scheduled_deletions WHERE message_id = ? AND chat_id = ?',
                   (message_id, chat_id))
    conn.commit()
    conn.close()

def save_channel_link(link, message_id, chat_id):
    conn = sqlite3.connect('messages.db')
    cursor = conn.cursor()
    cursor.execute('INSERT OR REPLACE INTO channel_links VALUES (?, ?, ?)',
                   (link, message_id, chat_id))
    conn.commit()
    conn.close()

def is_link_in_channel(link):
    conn = sqlite3.connect('messages.db')
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM channel_links WHERE link = ?', (link,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def mark_pinned_message_processed(chat_id, message_id):
    conn = sqlite3.connect('messages.db')
    cursor = conn.cursor()
    cursor.execute('INSERT OR REPLACE INTO pinned_messages VALUES (?, ?, 1)',
                   (chat_id, message_id))
    conn.commit()
    conn.close()

def is_pinned_message_processed(chat_id, message_id):
    conn = sqlite3.connect('messages.db')
    cursor = conn.cursor()
    cursor.execute('SELECT processed FROM pinned_messages WHERE chat_id = ? AND message_id = ?',
                   (chat_id, message_id))
    result = cursor.fetchone()
    conn.close()
    return result and result[0] == 1 if result else False

def extract_links(text):
    if not text:
        return []
    return re.findall(r'(?:https?://|t\.me/|@)\S+', text, re.IGNORECASE)

async def cache_existing_links():
    print("⏳ Кешування існуючих посилань з відстежуваного каналу...")
    try:
        async for message in client.iter_messages(monitored_channel_id):
            if message.text:
                links = extract_links(message.text)
                for link in links:
                    save_channel_link(link, message.id, monitored_channel_id)
        print("✅ Кешування посилань завершено")
    except Exception as e:
        print(f"❌ Помилка кешування посилань: {e}")

async def contains_monitored_channel_links(text):
    if not text:
        return False
    
    links = extract_links(text)
    if not links:
        return False
    
    for link in links:
        if is_link_in_channel(link):
            return True
    return False

def contains_keywords(text):
    if not text:
        return False
    text_lower = text.lower()
    return any(keyword.lower() in text_lower for keyword in keywords)

def contains_blacklisted_emojis(text):
    if not text:
        return False
    return any(emoji in text for emoji in blacklist_emojis)

def contains_blacklisted_words(text):
    if not text:
        return False
    return any(word.lower() in text.lower() for word in blacklist_words)

def contains_forbidden_numbers(text):
    if not text:
        return False
    return bool(re.search(r'(?:\d[\s]*){12,}', text))

def contains_any_links(text):
    if not text:
        return False
    return bool(re.search(r'(?:https?://|t\.me/|@)\S+', text, re.IGNORECASE))

def generate_appendix_content(current_text_length):
    invisible_spaces_text = "\u200B \u200B \u200B"
    radar_text_with_zero_width = "Радар\u200B"
    subscribe_text_with_zero_width = "Підписатись\u200B"
    separator_text = " • "

    appendix_text_raw = f"\n{invisible_spaces_text}\n{radar_text_with_zero_width}{separator_text}{subscribe_text_with_zero_width}"
    appendix_entities = []

    offset_link1 = current_text_length + 1
    appendix_entities.append(MessageEntityTextUrl(offset=offset_link1, length=len(invisible_spaces_text), url='https://t.me/+9lRifXZr51tiNDg0'))

    offset_link2 = offset_link1 + len(invisible_spaces_text) + 1
    appendix_entities.append(MessageEntityTextUrl(offset=offset_link2, length=len(radar_text_with_zero_width), url='https://t.me/+9lRifXZr51tiNDg0'))

    offset_link3 = offset_link2 + len(radar_text_with_zero_width) + len(separator_text)
    appendix_entities.append(MessageEntityTextUrl(offset=offset_link3, length=len(subscribe_text_with_zero_width), url='https://t.me/+9lRifXZr51tiNDg0'))

    return appendix_text_raw, appendix_entities

async def send_or_edit_combined_message(target_channel, original_message, source_channel_id, base_text, base_entities=None):
    processed_text = base_text if base_text else ''
    current_entities = []
    
    for emoji, emoji_id in EMOJI_REPLACEMENTS.items():
        if isinstance(emoji, str) and emoji in processed_text:
            emoji_pos = 0
            while emoji_pos != -1:
                emoji_pos = processed_text.find(emoji, emoji_pos)
                if emoji_pos != -1:
                    emoji_entity = MessageEntityCustomEmoji(
                        offset=emoji_pos,
                        length=len(emoji),
                        document_id=emoji_id
                    )
                    current_entities.append(emoji_entity)
                    emoji_pos += len(emoji)
    
    if base_entities:
        for entity in base_entities:
            if isinstance(entity, MessageEntityCustomEmoji):
                new_emoji_id = EMOJI_REPLACEMENTS.get(entity.document_id, entity.document_id)
                new_entity = MessageEntityCustomEmoji(
                    offset=entity.offset,
                    length=entity.length,
                    document_id=new_emoji_id
                )
                current_entities.append(new_entity)
            else:
                current_entities.append(entity)
    
    appendix_text_raw, appendix_entities = generate_appendix_content(len(processed_text))
    final_text = processed_text + appendix_text_raw
    final_entities = current_entities + appendix_entities

    channel_message_id = get_channel_message_id(original_message.id, source_channel_id, target_channel)

    if channel_message_id:
        try:
            await client.edit_message(
                target_channel,
                channel_message_id,
                final_text,
                formatting_entities=final_entities,
                link_preview=False
            )
            return channel_message_id
        except MessageNotModifiedError:
            return channel_message_id
        except Exception as e:
            print(f"❌ Помилка редагування: {e}")

    try:
        current_reply_to_id = None
        if original_message.reply_to_msg_id:
            current_reply_to_id = get_channel_message_id(original_message.reply_to_msg_id, source_channel_id, target_channel)

        if original_message.media:
            sent_message = await client.send_file(
                target_channel,
                file=original_message.media,
                caption=final_text,
                formatting_entities=final_entities,
                reply_to=current_reply_to_id
            )
        else:
            sent_message = await client.send_message(
                target_channel,
                final_text,
                formatting_entities=final_entities,
                reply_to=current_reply_to_id,
                link_preview=False
            )
        
        save_message_link(original_message.id, source_channel_id, target_channel, sent_message.id)
        return sent_message.id

    except ChatAdminRequiredError:
        print("❌ Акаунт не має прав на публікацію в цьому каналі.")
    except Exception as e:
        print(f"❌ Помилка відправки: {e}")

async def parse_and_schedule_deletions(message):
    text = message.text.strip()
    if not text.startswith('/del'):
        print("❌ Не команда /del")
        return
    
    print(f"🔍 Обробка команди /del: {text}")
    
    try:
        content = text[4:].strip()
        lines = [line.strip() for line in content.split('\n') if line.strip()]
        print(f"📝 Рядки для обробки: {lines}")
        
        default_time = None
        if lines and re.match(r'^\d{1,2}:\d{2}(?::\d{2})?$', lines[-1]):
            default_time = lines[-1]
            lines = lines[:-1]
            print(f"⏰ Встановлено час за замовчуванням: {default_time}")
        
        if not lines:
            print("❌ Немає рядків для обробки після вилучення часу")
            return
        
        scheduled_count = 0
        for line in lines:
            print(f"🔍 Обробка рядка: {line}")
            parts = re.split(r'\s+', line, 1)
            url = parts[0]
            time_str = (parts[1] if len(parts) > 1 and 
                       re.match(r'^\d{1,2}:\d{2}(?::\d{2})?$', parts[1]) 
                       else default_time)
            
            if not time_str:
                print(f"❌ Немає часу для рядка: {line}")
                continue
                
            print(f"🔗 URL: {url}, ⏰ Час: {time_str}")
            
            match = re.search(
                r'(?:https?://)?t\.me/(?:c/)?(\w+)/(\d+)',
                url,
                re.IGNORECASE
            )
            
            if not match:
                print(f"❌ Невірний формат URL: {url}")
                continue
                
            channel_username = match.group(1)
            message_id = int(match.group(2))
            
            try:
                channel = await client.get_entity(f"t.me/{channel_username}")
                chat_id = channel.id
                print(f"📌 Канал знайдено: {channel_username} (ID: {chat_id})")
            except Exception as e:
                print(f"❌ Помилка отримання каналу {channel_username}: {e}")
                continue
            
            print(f"📌 Заплановано видалення: chat_id={chat_id}, message_id={message_id}, time={time_str}")
            
            if await schedule_message_deletion(message, chat_id, message_id, time_str, message.id):
                scheduled_count += 1
                print(f"✅ Успішно заплановано видалення {message_id} з {chat_id}")
        
        if scheduled_count > 0:
            log_msg = await client.send_message(
                deletion_command_channel_id,
                f"✔️ Успішно заплановано {scheduled_count} видалень",
                reply_to=message.id
            )
            pending_del_commands[message.id] = {
                'scheduled_count': scheduled_count,
                'deleted_count': 0,
                'log_message_id': log_msg.id
            }
            print(f"📝 Лог-повідомлення створено: {log_msg.id}")
            asyncio.create_task(delete_message_after_delay(log_msg.id, deletion_command_channel_id, 30))
        
    except Exception as e:
        print(f"❌ Критична помилка при обробці /del: {str(e)}")
        raise

async def delete_message_after_delay(message_id, chat_id, delay):
    try:
        await asyncio.sleep(delay)
        await client.delete_messages(chat_id, message_id)
    except Exception as e:
        print(f"❌ [ПОМИЛКА ВИДАЛЕННЯ] Не вдалося видалити повідомлення {message_id} з чату {chat_id} після затримки: {e}")

async def schedule_message_deletion(message, chat_id, message_id, time_str, original_del_command_id):
    try:
        print(f"⏳ Заплановано видалення повідомлення {message_id} з чату {chat_id} на {time_str}")
        now = datetime.now(KYIV_TZ)
        
        time_parts = list(map(int, time_str.split(':')))
        hour, minute = time_parts[:2]
        second = time_parts[2] if len(time_parts) > 2 else 0
        
        delete_at_naive = now.replace(hour=hour, minute=minute, second=second, microsecond=0, tzinfo=None)
        delete_at = KYIV_TZ.localize(delete_at_naive)

        if delete_at < now:
            delete_at += timedelta(days=1)
            
        conn = sqlite3.connect('messages.db')
        cursor = conn.cursor()
        cursor.execute('SELECT log_message_id FROM scheduled_deletions WHERE message_id = ? AND chat_id = ?',
                       (message_id, chat_id))
        existing_log_msg_id = cursor.fetchone()
        conn.close()

        log_msg_id_to_save = None
        scheduled_text = (
            f"**⏳Заплановано до видалення повідомлення:**\n"
            f"  ID: `{message_id}`\n"
            f"  У каналі: `{get_channel_name(chat_id)}` (`{chat_id}`)\n"
            f"  ⏰: `{delete_at.strftime('%H:%M:%S')}`"
        )

        if existing_log_msg_id and existing_log_msg_id[0]:
            log_msg_id_to_save = existing_log_msg_id[0]
            try:
                await client.edit_message(
                    deletion_command_channel_id,
                    log_msg_id_to_save,
                    scheduled_text,
                    parse_mode='markdown'
                )
            except Exception:
                pass
        else:
            try:
                log_msg = await client.send_message(
                    deletion_command_channel_id,
                    scheduled_text,
                    reply_to=message.id,
                    parse_mode='markdown'
                )
                log_msg_id_to_save = log_msg.id
            except Exception as e:
                print(f"❌ Помилка створення лог-повідомлення: {e}")
        
        save_scheduled_deletion(message_id, chat_id, delete_at.isoformat(), log_msg_id_to_save, original_del_command_id)
        scheduled_deletions[(message_id, chat_id)] = (delete_at, log_msg_id_to_save, original_del_command_id)
        
        return True
        
    except Exception as e:
        print(f"❌ Помилка планування: {str(e)}")
        return False

async def check_and_perform_deletions():
    while True:
        try:
            now = datetime.now(KYIV_TZ)
            current_scheduled_deletions = list(scheduled_deletions.items())
            
            for (message_id, chat_id), (delete_at, log_msg_id, original_del_command_id) in current_scheduled_deletions:
                if now >= delete_at:
                    try:
                        await client.delete_messages(chat_id, message_id)
                        
                        if log_msg_id:
                            deleted_text = (
                                f"**✅Видалено повідомлення:**\n"
                                f"  ID: `{message_id}`\n"
                                f"  З каналу: `{get_channel_name(chat_id)}` (`{chat_id}`)\n"
                                f"  ⏰: `{now.strftime('%H:%M:%S')}`"
                            )
                            try:
                                await client.edit_message(
                                    deletion_command_channel_id,
                                    log_msg_id,
                                    deleted_text,
                                    parse_mode='markdown'
                                )
                                asyncio.create_task(delete_message_after_delay(log_msg_id, deletion_command_channel_id, 300))
                            except Exception as log_e:
                                print(f"❌ [ПОМИЛКА ЛОГУ] Не вдалося оновити лог-повідомлення {log_msg_id}: {log_e}")
                        
                        delete_scheduled_deletion(message_id, chat_id)
                        del scheduled_deletions[(message_id, chat_id)]
                        
                        if original_del_command_id in pending_del_commands:
                            pending_del_commands[original_del_command_id]['deleted_count'] += 1
                            if (pending_del_commands[original_del_command_id]['deleted_count'] >= 
                                pending_del_commands[original_del_command_id]['scheduled_count']):
                                try:
                                    await client.delete_messages(deletion_command_channel_id, original_del_command_id)
                                except Exception as del_cmd_e:
                                    print(f"❌ [ПОМИЛКА ВИДАЛЕННЯ] Не вдалося видалити оригінальну команду /del {original_del_command_id}: {del_cmd_e}")
                                del pending_del_commands[original_del_command_id]

                    except ChatAdminRequiredError:
                        print(f"❌ [ПОМИЛКА ПРАВ] Акаунт не має прав адміністратора для видалення повідомлення {message_id} у чаті {chat_id}.")
                    except Exception as e:
                        print(f"❌ [ПОМИЛКА ВИДАЛЕННЯ] Не вдалося видалити повідомлення {message_id} з чату {chat_id}: {e}")
            
            await asyncio.sleep(1) 
        except Exception as e:
            print(f"❌ [КРИТИЧНА ПОМИЛКА] Помилка перевірки запланованих видалень: {e}")
            await asyncio.sleep(5)

@client.on(events.NewMessage(chats=deletion_command_channel_id))
async def handle_del_command(event):
    message = event.message
    if message.text and message.text.startswith('/del'):
        try:
            await client.delete_messages(message.chat_id, message.id)
            await parse_and_schedule_deletions(message)
        except Exception as e:
            print(f"❌ Помилка при видаленні команди /del: {e}")

def contains_any_links(text, entities=None):
    if not text:
        return False
    
    if re.search(r'(?:https?://|t\.me/|@)\S+', text, re.IGNORECASE):
        return True
    
    if entities:
        for entity in entities:
            if isinstance(entity, MessageEntityTextUrl):
                return True
    
    return False

@client.on(events.NewMessage(chats=source_channels))
async def handler(event):
    message = event.message
    source_channel_id = event.chat_id
    
    if message.is_reply:
        replied_msg = await message.get_reply_message()
        if replied_msg and (replied_msg.voice or replied_msg.video_note):
            log_rejection("є відповіддю на голосове повідомлення або відеокружок", message)
            return

    original_text = message.message or ''
    original_entities = list(message.entities) if message.entities else []

    if contains_any_links(original_text, original_entities):
        if source_channel_id != unrestricted_channel_id:
            log_rejection("містить посилання", message)
            return
        
        if await contains_monitored_channel_links(original_text):
            log_rejection("містить посилання з моніторингового каналу", message)
            return

    if source_channel_id != unrestricted_channel_id:
        if not contains_keywords(original_text):
            log_rejection("не містить ключових слів", message)
            return
        if message.media:
            log_rejection("містить медіа-вміст", message)
            return
        if contains_blacklisted_words(original_text):
            log_rejection("містить заборонені слова", message)
            return
        if contains_blacklisted_emojis(original_text):
            log_rejection("містить заборонені емодзі", message)
            return
        if contains_forbidden_numbers(original_text):
            log_rejection("містить заборонені цифрові послідовності", message)
            return

    try:
        fresh_msg = await client.get_messages(source_channel_id, ids=message.id)
        if fresh_msg is None:
            log_rejection("повідомлення було видалено адміністратором", message)
            return
    except Exception as e:
        print(f"❌ Помилка перевірки існування повідомлення: {e}")
        return

    if source_channel_id == monitored_channel_id and original_text:
        links = extract_links(original_text)
        for link in links:
            save_channel_link(link, message.id, source_channel_id)

    for target_channel in target_channel_ids:
        if get_channel_message_id(message.id, source_channel_id, target_channel):
            continue
            
        await send_or_edit_combined_message(
            target_channel,
            message,
            source_channel_id,
            original_text,
            original_entities
        )

@client.on(events.MessageEdited(chats=source_channels))
async def handler_edit(event):
    message = event.message
    source_channel_id = event.chat_id
    
    if message.is_reply:
        replied_msg = await message.get_reply_message()
        if replied_msg and (replied_msg.voice or replied_msg.video_note):
            log_rejection("є відповіддю на голосове повідомлення або відеокружок", message)
            return

    original_text = message.message or ''
    original_entities = list(message.entities) if message.entities else []

    if contains_any_links(original_text, original_entities):
        if source_channel_id != unrestricted_channel_id:
            log_rejection("містить посилання", message)
            return
        
        if await contains_monitored_channel_links(original_text):
            log_rejection("містить посилання з моніторингового каналу", message)
            return

    if source_channel_id != unrestricted_channel_id:
        if not contains_keywords(original_text):
            log_rejection("не містить ключових слів", message)
            return
        if message.media:
            log_rejection("містить медіа-вміст", message)
            return
        if contains_blacklisted_words(original_text):
            log_rejection("містить заборонені слова", message)
            return
        if contains_blacklisted_emojis(original_text):
            log_rejection("містить заборонені емодзі", message)
            return
        if contains_forbidden_numbers(original_text):
            log_rejection("містить заборонені цифрові послідовності", message)
            return

    if source_channel_id == monitored_channel_id and original_text:
        links = extract_links(original_text)
        for link in links:
            save_channel_link(link, message.id, source_channel_id)

    for target_channel in target_channel_ids:
        if not get_channel_message_id(message.id, source_channel_id, target_channel):
            continue
            
        await send_or_edit_combined_message(
            target_channel,
            message,
            source_channel_id,
            original_text,
            original_entities
        )

@client.on(events.MessageDeleted(chats=source_channels))
async def handler_delete(event):
    source_channel_id = event.chat_id
    for msg_id in event.deleted_ids:
        for target_channel in target_channel_ids:
            message_id = get_channel_message_id(msg_id, source_channel_id, target_channel)
            if message_id:
                try:
                    await client.delete_messages(target_channel, message_id)
                except Exception as e:
                    print(f"❌[ПОМИЛКА] Не вдалося видалити: {e}")

async def unpin_message_with_retry(chat_id, message_id, retries=3, default_delay=882):
    if message_id == 7733:
        print(f"🚫 Повідомлення {message_id} ігнорується (заборонено відкріплювати)")
        return False

    for attempt in range(retries):
        try:
            await client.unpin_message(chat_id, message_id)
            print(f"✅ Повідомлення {message_id} успішно відкріплено на спробі {attempt + 1}")
            return True
        except Exception as e:
            error_str = str(e)
            if "A wait of" in error_str:
                match = re.search(r'A wait of (\d+) seconds', error_str)
                delay = int(match.group(1)) if match else default_delay
                print(f"❌ Rate limit, зачекайте {delay} секунд перед повторною спробою... (Спроба {attempt + 1}/{retries})")
                await asyncio.sleep(delay)
            else:
                print(f"❌ Помилка відкріплення повідомлення {message_id}: {e}")
                return False
    print(f"❌ Не вдалося відкріпити повідомлення {message_id} після {retries} спроб")
    return False

async def monitor_pinned_messages():
    while True:
        try:
            for chat_id in pinned_monitoring_chats:
                try:
                    chat = await client.get_entity(chat_id)
                    if not hasattr(chat, 'megagroup') or not chat.megagroup:
                        continue
                        
                    async for msg in client.iter_messages(chat_id, limit=100):
                        if msg.pinned and not is_pinned_message_processed(chat_id, msg.id) and msg.id != 7733:
                            print(f"⚠️ Виявлено нове закріплене повідомлення {msg.id} в групі {chat_id}")
                            
                            try:
                                if await unpin_message_with_retry(chat_id, msg.id):
                                    print(f"✅ Повідомлення {msg.id} у групі {chat_id} успішно відкріплено")
                                    mark_pinned_message_processed(chat_id, msg.id)
                                await asyncio.sleep(2)
                            except Exception as e:
                                print(f"❌ Помилка відкріплення: {e}")
                except Exception as e:
                    print(f"❌ Помилка моніторингу групи {chat_id}: {e}")
            
            await asyncio.sleep(5)
        except Exception as e:
            print(f"❌ Критична помилка моніторингу: {e}")
            await asyncio.sleep(10)

async def unpin_existing_messages():
    for chat_id in pinned_monitoring_chats:
        try:
            chat = await client.get_entity(chat_id)
            if not hasattr(chat, 'megagroup') or not chat.megagroup:
                print(f"⚠️ {chat_id} не є групою, пропускаємо")
                continue

            offset_id = 0
            limit = 5
            total_unpinned = 0
            while True:
                pinned_messages = []
                async for msg in client.iter_messages(chat_id, limit=limit, offset_id=offset_id):
                    if msg.pinned and not is_pinned_message_processed(chat_id, msg.id) and msg.id != 7733:
                        pinned_messages.append(msg)
                if not pinned_messages:
                    break
                print(f"🔍 Знайдено {len(pinned_messages)} закріплених повідомлень у групі {chat_id} (offset {offset_id})")
                for msg in pinned_messages:
                    try:
                        if await unpin_message_with_retry(chat_id, msg.id):
                            mark_pinned_message_processed(chat_id, msg.id)
                            print(f"📌 Відкріплено повідомлення {msg.id} з групи {chat_id}")
                            total_unpinned += 1
                        await asyncio.sleep(10)
                    except Exception as e:
                        print(f"❌ Помилка відкріплення повідомлення {msg.id}: {e}")
                offset_id = pinned_messages[-1].id
                await asyncio.sleep(10)
            print(f"✅ Завершено: Відкріплено {total_unpinned} повідомлень у групі {chat_id}")
        except Exception as e:
            print(f"❌ Помилка при отриманні закріплених повідомлень для групи {chat_id}: {e}")

async def main():
    init_db()
    
    clear_pinned_messages_db(-1002880304508)
    
    await client.start(phone_number)
    print("🟢 Скрипт запущено")
    
    for chat_id in pinned_monitoring_chats:
        try:
            chat = await client.get_entity(chat_id)
            if not hasattr(chat, 'megagroup') or not chat.megagroup:
                print(f"⚠️ Увага! {chat_id} не є групою (мегагрупою)")
        except Exception as e:
            print(f"❌ Помилка перевірки групи {chat_id}: {e}")
    
    for msg_id, chat_id, del_at_str, log_id, orig_del_id in get_scheduled_deletions():
        try:
            delete_at = datetime.fromisoformat(del_at_str).astimezone(KYIV_TZ)
            scheduled_deletions[(msg_id, chat_id)] = (delete_at, log_id, orig_del_id)
            if orig_del_id not in pending_del_commands:
                pending_del_commands[orig_del_id] = {'scheduled_count': 0, 'deleted_count': 0, 'log_message_id': None}
            pending_del_commands[orig_del_id]['scheduled_count'] += 1
        except (ValueError, TypeError) as e:
            print(f"❌ [ПОМИЛКА ЗАВАНТАЖЕННЯ] Не вдалося завантажити заплановане видалення {msg_id}: {e}")

    asyncio.create_task(check_and_perform_deletions())
    asyncio.create_task(monitor_pinned_messages())
    
    await unpin_existing_messages()
    
    await cache_existing_links()
    
    await client.run_until_disconnected()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🔴 Скрипт зупинено вручну")
    except Exception as e:
        print(f"❌ Критична помилка: {e}")
