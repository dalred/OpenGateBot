import os
import re
import time
import asyncio
import uvloop
import pytz
import ssl
from datetime import datetime, time as dtime
from datetime import datetime
from dotenv import load_dotenv
from telegram import Update, KeyboardButton, ReplyKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    ConversationHandler,
    CallbackQueryHandler,
    filters,
)
from telegram import ReplyKeyboardRemove
from telegram.error import NetworkError
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import InlineKeyboardMarkup, InlineKeyboardButton

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
SHEET_ID = os.getenv("SHEET_ID")
GOOGLE_CREDENTIALS_FILE = "credentials.json"
ASK_NAME, ASK_PHONE = range(2)
DOMAIN_IP = os.getenv("DOMAIN_IP")
#


def check_access_time(access_time_str: str) -> bool:
    tz = pytz.timezone("Europe/Moscow")
    now = datetime.now(tz)
    current_day = now.strftime("%a").lower()  # например, 'mon'
    current_time = now.time()

    if access_time_str.strip().lower() == "always":
        return True

    try:
        days_part, time_range = access_time_str.strip().split()
        start_str, end_str = time_range.split("-")
        start_time = datetime.strptime(start_str, "%H:%M").time()
        end_time = datetime.strptime(end_str, "%H:%M").time()

        # Расширяем дни: 'mon-fri', 'sat', 'sun' и т.п.
        day_map = {
            "mon": 0,
            "tue": 1,
            "wed": 2,
            "thu": 3,
            "fri": 4,
            "sat": 5,
            "sun": 6,
            "weekdays": (0, 1, 2, 3, 4),
            "weekends": (5, 6),
        }

        allowed_days = set()
        for part in days_part.split(","):
            part = part.strip().lower()
            if part in ("weekdays", "weekends"):
                allowed_days.update(day_map[part])
            elif "-" in part:
                start_day, end_day = part.split("-")
                start_idx = day_map[start_day]
                end_idx = day_map[end_day]
                for i in range(start_idx, end_idx + 1):
                    allowed_days.add(i)
            elif part in day_map:
                allowed_days.add(day_map[part])

        today_idx = now.weekday()

        if today_idx in allowed_days:
            if start_time <= current_time <= end_time:
                return True
    except Exception as e:
        log(f"[⚠️] Ошибка в access_time: {e}")

    return False


def log(msg):
    now = datetime.now()
    timestamp = now.strftime("%d.%m.%Y %H:%M:%S")
    log_line = f"[{timestamp}] {msg}"

    # Путь к лог-файлу по дате, например: logs/2025-05-13.log
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_filename = os.path.join(log_dir, f"{now.strftime('%d-%m-%Y')}.log")

    # Пишем в файл + выводим в консоль
    with open(log_filename, "a", encoding="utf-8") as f:
        f.write(log_line + "\n")

    print(log_line)


def normalize_phone(phone):
    return re.sub(r"\D", "", str(phone))[-10:] if phone else ""


def safe_gspread_call(func, *args, retries=3, delay=2, **kwargs):
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            log(f"[⚠️] GSpread error ({attempt}/{retries}): {e}")
            time.sleep(delay)
    log(f"[❌] Не удалось выполнить {func.__name__} после {retries} попыток.")
    return None


def safe_get_all_records(sheet):
    return safe_gspread_call(sheet.get_all_records) or []


def safe_update_cell(sheet, row, col, value):
    return safe_gspread_call(sheet.update_cell, row, col, value)


def safe_append_row(sheet, row_values, value_input_option="USER_ENTERED"):
    return safe_gspread_call(
        sheet.append_row, row_values, value_input_option=value_input_option
    )


def get_sheet(retries=3, delay=2):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    for attempt in range(1, retries + 1):
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                GOOGLE_CREDENTIALS_FILE, scope
            )
            client = gspread.authorize(creds)
            return client.open_by_key(SHEET_ID).worksheet("AccessList")
        except Exception as e:
            log(f"[⚠️] Google Sheets error ({attempt}/{retries}): {e}")
            time.sleep(delay)
    return None


def get_user_status(user_id: str) -> str:
    sheet = get_sheet()
    if not sheet:
        return "none"
    records = safe_get_all_records(sheet)
    for row in records:
        if str(row.get("user_id")) == user_id:
            return row.get("aprove", "").strip().lower() or "none"
    return "none"


def get_main_menu(status: str = "none"):
    if status == "yes":
        return ReplyKeyboardMarkup(
            [
                ["🔓 Открыть/закрыть калитку", "🔁 Изменить номер"],
                ["ℹ️ Помощь", "🏁 Начало"],
            ],
            resize_keyboard=True,
        )
    elif status == "no":
        return ReplyKeyboardMarkup(
            [["🔄 Проверить статус", "ℹ️ Помощь", "🏁 Начало"]],
            resize_keyboard=True,
        )
    elif status == "pending":
        return ReplyKeyboardMarkup(
            [["🔄 Проверить статус", "🔁 Изменить номер", "ℹ️ Помощь", "🏁 Начало"]],
            resize_keyboard=True,
        )
    else:
        return ReplyKeyboardMarkup(
            [["📋 Зарегистрироваться"], ["🔄 Проверить статус", "ℹ️ Помощь", "🏁 Начало"]],
            resize_keyboard=True,
        )


async def safe_reply(message, text, retries=3, delay=2, **kwargs):
    for attempt in range(retries):
        try:
            return await message.reply_text(text, **kwargs)
        except NetworkError as e:
            log(f"[⚠️] NetworkError ({attempt+1}/{retries}): {type(e).__name__} — {e}")
            await asyncio.sleep(delay)
    log("[❌] Не удалось отправить сообщение после повторов.")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = str(user.id)
    name = user.first_name or user.username or "пользователь"

    status = "none"
    sheet = get_sheet()
    if sheet:
        records = safe_get_all_records(sheet)
        for row in records:
            if str(row.get("user_id")) == user_id:
                status = row.get("aprove", "").strip().lower()
                break

    await safe_reply(
        update.message,
        f"👋 Привет, {name}! Выберите действие:",
        reply_markup=get_main_menu(status),
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_reply(
        update.message,
        "ℹ️ Доступные команды:\n/start — начать\n📋 Зарегистрироваться\n🔁 Изменить номер\n🔄 Проверить статус\nℹ️ Помощь — информация об администраторе",
    )


async def help_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_reply(
        update.message,
        "ℹ️ По всем вопросам обращайтесь к администратору:\n@DanielPython",
    )


async def register_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text(
        "Введите вашу Фамилию и Имя:",
        reply_markup=ReplyKeyboardRemove(),  # ⬅️ Скрываем клавиатуру
    )
    return ASK_NAME


async def ask_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["fio"] = update.message.text.strip()
    button = KeyboardButton("📱 Отправить номер", request_contact=True)
    keyboard = ReplyKeyboardMarkup(
        [[button]], resize_keyboard=True, one_time_keyboard=True
    )
    await safe_reply(
        update.message, "Теперь отправьте номер телефона:", reply_markup=keyboard
    )
    return ASK_PHONE


async def change_phone_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["change_mode"] = True
    button = KeyboardButton("📱 Отправить новый номер", request_contact=True)
    keyboard = ReplyKeyboardMarkup(
        [[button]], resize_keyboard=True, one_time_keyboard=True
    )
    await safe_reply(update.message, "⬇️ Отправьте новый номер:", reply_markup=keyboard)
    return ASK_PHONE


async def ask_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    sheet = get_sheet()
    if not sheet:
        await safe_reply(update.message, "❌ Ошибка доступа к таблице.")
        return ConversationHandler.END

    contact = update.message.contact
    text = update.message.text
    user_id = str(user.id)
    phone = None

    if contact and contact.phone_number:
        phone = contact.phone_number
    elif text and re.fullmatch(r"\+?\d{10,15}", text.strip()):
        phone = text.strip()
    else:
        await safe_reply(
            update.message, "⚠️ Введите номер корректно или используйте кнопку."
        )
        return ASK_PHONE

    phone = normalize_phone(phone)
    records = safe_get_all_records(sheet)

    # === Смена номера ===
    if context.user_data.get("change_mode"):
        for i, row in enumerate(records, start=2):
            if str(row.get("user_id")) == user_id:
                old_phone = normalize_phone(row.get("phone", ""))
                if phone == old_phone:
                    log(
                        f"[🔁] {user_id} отправил тот же номер ({phone}), статус не изменён"
                    )
                    status = get_user_status(user_id)
                    await safe_reply(
                        update.message,
                        "ℹ️ Вы отправили тот же номер. Изменений не внесено.",
                        reply_markup=get_main_menu(status),
                    )
                    return ConversationHandler.END

                safe_update_cell(sheet, i, 4, phone)
                safe_update_cell(sheet, i, 5, "pending")
                log(f"[🔁] {user_id} сменил номер на {phone}, статус сброшен")
                status = get_user_status(user_id)
                await safe_reply(
                    update.message,
                    "✅ Номер успешно обновлён! Заявка отправлена повторно, ожидайте одобрения.",
                    reply_markup=get_main_menu(status),
                )
                return ConversationHandler.END

        await safe_reply(
            update.message, "⚠️ Не удалось найти вашу заявку для обновления."
        )
        return ConversationHandler.END

    # === Регистрация (если не найден user_id) ===
    for row in records:
        if str(row.get("user_id")) == user_id:
            log(
                f"[ℹ️] Повторная попытка — уже зарегистрирован: {user_id}, phone: {phone}"
            )
            await safe_reply(
                update.message,
                "✅ Вы уже зарегистрированы.",
                reply_markup=get_main_menu(),
            )
            return ConversationHandler.END

    fio = context.user_data.get("fio", "")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    telegram_link = f"https://t.me/{user.username}" if user.username else ""
    safe_append_row(
        sheet,
        [
            user_id,
            user.username or "",
            fio,
            phone,
            "pending",
            "sat 08:00-19:00",
            timestamp,
            telegram_link,
        ],
        value_input_option="USER_ENTERED",
    )
    log(f"[📋] Новая заявка от {user_id}: {fio}, {phone}")
    context.user_data["is_registering"] = False
    # 👇 Отправка админу
    admin_chat_id = int(os.getenv("ADMIN_CHAT_ID"))
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✅ Подтвердить", callback_data=f"approve:{user_id}"
                ),
                InlineKeyboardButton("❌ Отклонить", callback_data=f"reject:{user_id}"),
                InlineKeyboardButton("🕓 Оставить в ожидании", callback_data="pending"),
            ]
        ]
    )

    await context.bot.send_message(
        chat_id=admin_chat_id,
        text=(
            f"👤 Пользователь *{fio}* (`{user_id}`) просит доступ\n"
            f"🔗 [Профиль](https://t.me/{user.username})"
        ),
        reply_markup=keyboard,
        parse_mode="Markdown",
    )

    # Ответ пользователю: скрываем клавиатуру и говорим, что заявка обрабатывается
    await safe_reply(
        update.message,
        "📨 Заявка обрабатывается. Пожалуйста, подождите подтверждения от администратора.",
        reply_markup=get_main_menu("pending"),
    )

    return ConversationHandler.END


async def check_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    user_name = str(update.effective_user.username)
    sheet = get_sheet()
    if not sheet:
        await safe_reply(update.message, "❌ Ошибка подключения к таблице.")
        return

    records = safe_get_all_records(sheet)
    for row in records:
        if str(row.get("user_id")) == user_id:
            status = row.get("aprove", "").strip().lower()
            if status == "yes":
                log(
                    f"[✅] Доступ разрешён — user_id: {user_id}, phone: {row.get('phone', '')}"
                )
                await safe_reply(
                    update.message,
                    "✅ Ваша заявка одобрена. Доступ разрешён.",
                    reply_markup=get_main_menu("yes"),
                )
            elif status == "no":
                log(
                    f"[❌] Отклонено — user_id: {user_id}, phone: {row.get('phone', '')}, username: {row.get('username', '')}"
                )
                await safe_reply(
                    update.message,
                    "❌ Ваша заявка была отклонена.\nВы можете отправить номер заново или обратиться к администратору: @DanielPython",
                    reply_markup=get_main_menu("no"),
                )
            else:  # pending
                log(f"[⏳] Заявка рассматривается — user_id: {user_id}")
                await safe_reply(
                    update.message,
                    "⏳ Заявка ещё рассматривается.",
                    reply_markup=get_main_menu("pending"),
                )
            return

    log(f"ℹ️ user_id={user_id}, {user_name} Вы ещё не подавали заявку.")
    await safe_reply(update.message, "ℹ️ Вы ещё не подавали заявку.")


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_reply(update.message, "🚫 Регистрация отменена.")
    return ConversationHandler.END


async def unknown_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_reply(update.message, "❓ Пожалуйста, используйте кнопки меню.")


async def my_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await update.message.reply_text(
        f"Ваш `chat_id`: `{chat_id}`", parse_mode="Markdown"
    )


async def open_gate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = str(user.id)
    username = user.username or "unknown"

    sheet = get_sheet()
    if not sheet:
        log(f"❌ Ошибка подключения к Google Sheets")
        await safe_reply(update.message, "❌ Ошибка доступа к таблице.")
        return

    records = safe_get_all_records(sheet)
    for row in records:
        if str(row.get("user_id")) == user_id:
            status = row.get("aprove", "").strip().lower()
            access_time = str(row.get("access_time", "always")).strip().lower()
            if status == "yes":
                if check_access_time(access_time):
                    log(f"[🔓] Разрешённый доступ: user_id={user_id}, time OK")
                    log(
                        f"[🔓] Калитка открыта по запросу: user_id={user.id}, username={user.username}"
                    )
                    await safe_reply(
                        update.message,
                        "🚪 Калитка открывается/закрывается..(заглушка)",
                    )
                else:
                    log(
                        f"[⏰] Попытка доступа к калитке вне времени: user_id={user_id},username={user.username} access_time={access_time}"
                    )
                    await safe_reply(
                        update.message,
                        "🕒 Доступ к калитке возможен только в разрешённое время.",
                    )
                return
            else:
                log(f"[⛔] Доступ запрещён — user_id: {user_id}, статус: {status}")
                await safe_reply(update.message, "🚫 Ваш доступ ещё не подтверждён.")
                return
    log(f"[❌] Пользователь не найден — user_id: {user_id}, username: {username}")
    await safe_reply(update.message, "🚫 Вы не зарегистрированы.")


async def notify_admin_about_request(
    user_id: str, fio: str, username: str, context: ContextTypes.DEFAULT_TYPE
):
    admin_chat_id = int(os.getenv("ADMIN_CHAT_ID", "YOUR_CHAT_ID"))

    link = f"https://t.me/{username}" if username else "нет ссылки"
    text = (
        f"📩 Новый запрос на доступ:\n\n"
        f"👤 ФИО: {fio}\n"
        f"🆔 user_id: {user_id}\n"
        f"🔗 Профиль: {link}\n\n"
        f"🕒 Статус: ⏳ Ожидание решения"
    )

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✅ Подтвердить", callback_data=f"approve:{user_id}"
                ),
                InlineKeyboardButton("❌ Отклонить", callback_data=f"reject:{user_id}"),
            ]
        ]
    )

    await context.bot.send_message(
        chat_id=admin_chat_id, text=text, reply_markup=keyboard, parse_mode="HTML"
    )


async def handle_admin_decision(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data
    sheet = get_sheet()
    if not sheet:
        await query.edit_message_text("❌ Ошибка доступа к таблице.")
        return

    if ":" not in data:
        await query.edit_message_text("ℹ️ Решение отложено.")
        return

    action, user_id = data.split(":", 1)
    records = safe_get_all_records(sheet)

    for i, row in enumerate(records, start=2):
        if str(row.get("user_id")) == user_id:
            fio = row.get("fio", "Неизвестно")
            username = row.get("username", "")
            mention = f"@{username}" if username else f"user_id={user_id}"

            if action == "approve":
                safe_update_cell(sheet, i, 5, "yes")
                log(f"[✅] Пользователь одобрен — {fio} ({mention})")
                await query.edit_message_text(
                    f"✅ Пользователь {fio} ({mention}) одобрен."
                )
                await context.bot.send_message(
                    chat_id=int(user_id),
                    text="✅ Ваша заявка одобрена! Доступ открыт. Добро пожаловать!",
                    reply_markup=get_main_menu("yes"),
                )
            elif action == "reject":
                safe_update_cell(sheet, i, 5, "no")
                log(f"[❌] Пользователь отклонён — {fio} ({mention})")
                await query.edit_message_text(
                    f"❌ Пользователь {fio} ({mention}) отклонён."
                )
            return

    await query.edit_message_text("⚠️ Пользователь не найден в таблице.")


async def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex("📋 Зарегистрироваться"), register_start),
            MessageHandler(filters.Regex("🔁 Изменить номер"), change_phone_start),
        ],
        states={
            ASK_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_name)],
            ASK_PHONE: [
                MessageHandler(filters.CONTACT, ask_phone),
                MessageHandler(filters.TEXT & ~filters.COMMAND, ask_phone),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    app.add_handler(conv_handler)
    app.add_handler(CallbackQueryHandler(handle_admin_decision))
    app.add_handler(CommandHandler("myid", my_id))
    app.add_handler(MessageHandler(filters.Regex("🏁 Начало"), start))
    app.add_handler(
        MessageHandler(filters.Regex("🔓 Открыть/закрыть калитку"), open_gate)
    )
    app.add_handler(
        MessageHandler(filters.Regex("🔄 Проверить статус"), check_status)
    )  # ⬅️ сюда
    app.add_handler(MessageHandler(filters.Regex("ℹ️ Помощь"), help_button))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unknown_input))
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))

    log("🤖 Бот запущен. Введите /start в Telegram.")
    await app.initialize()
    await app.start()

    mode = os.getenv("MODE", "polling")
    if mode == "webhook":
        cert_path = os.path.abspath("certs/webhook.crt")  # путь до публичного ключа
        privkey_path = os.path.abspath("certs/webhook.key")  # путь до приватного ключа

        await app.bot.set_webhook(
            url=f"https://{DOMAIN_IP}:8443",
            certificate=open(cert_path, "rb"),  # только если нужен сертификат
        )

        await app.run_webhook(
            listen="0.0.0.0",
            port=8443,
            url_path="",
            cert=cert_path,
            key=privkey_path,
            webhook_url=f"https://{DOMAIN_IP}:8443",
        )
    else:
        await app.updater.start_polling()
        await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass

    asyncio.run(main())
