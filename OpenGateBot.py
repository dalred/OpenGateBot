import os
import re
import time
import asyncio
import pytz
import json, random
from datetime import datetime, timezone
from datetime import datetime, time as dtime
from datetime import datetime
from datetime import datetime, timedelta
from dateutil.parser import isoparse


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
import paho.mqtt.publish as publish
import paho.mqtt.client as mqtt
from typing import Optional

load_dotenv()
moscow = pytz.timezone("Europe/Moscow")
min_interval_seconds = int(os.getenv("MIN_INTERVAL_SECONDS", "7"))
ARDUINO_CONFIRM_TIMEOUT = int(os.getenv("ARDUINO_CONFIRM_TIMEOUT", "10"))
MIN_INTERVAL = timedelta(seconds=min_interval_seconds)
last_used_time = {}
BOT_TOKEN = os.getenv("BOT_TOKEN")
SHEET_ID = os.getenv("SHEET_ID")
GOOGLE_CREDENTIALS_FILE = "credentials.json"
MQTT_USER = os.getenv("user_mosquitto")
MQTT_PASS = os.getenv("password_mosquitto")
HOST = os.getenv("HOST")
DOMAIN_IP = os.getenv("DOMAIN_IP")
MODE = os.getenv("MODE")


ASK_NAME, ASK_PHONE = range(2)


def log(msg):
    now = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    print(f"[{now}] {msg}")


async def is_too_soon(update, context) -> bool:
    """Проверка антифлуда: возвращает True, если пользователь нажал слишком быстро."""
    now = datetime.now()
    last_used = context.user_data.get("last_gate_call")

    if last_used and now - last_used < MIN_INTERVAL:
        await update.message.reply_text(
            f"⚠️ Подождите {MIN_INTERVAL.total_seconds():.0f} секунд перед повторной попыткой."
        )
        user_id = update.effective_user.id
        log(f"❌ Повторное открытие пользователем: user_id={user_id}")
        return True

    context.user_data["last_gate_call"] = now
    return False


gate_state = {"current": "IDLE"}


def process_gate_status(data, context):
    try:
        context.bot_data["last_gate_status"] = {
            "status": data["status"],
            "user_id": str(data["user_id"]),
            "timestamp": isoparse(data["timestamp"]),
        }
        # log(f"[📥] Arduino status: {data['status']} от user_id={data['user_id']}")
    except Exception as e:
        log(f"[❌] Ошибка обработки status от Arduino: {e}")


async def wait_for_arduino_confirmation(
    context,
    user_id: str,
    update: Update,
    command_name: str,
    timeout: int = ARDUINO_CONFIRM_TIMEOUT,
):
    await update.message.reply_text(
        f"📤 Команда отправлена. Ожидаем подтверждение от калитки...",
        # f"📤 Команда {command_name} отправлена. Ожидаем подтверждение от калитки...",
        disable_notification=True,  # 🔕 бесшумно
    )

    await asyncio.sleep(timeout)

    last_status = context.bot_data.get("last_gate_status")
    last_time = context.user_data.get("last_command_timestamp")

    if (
        not last_status
        or last_status.get("user_id") != user_id
        or last_status.get("timestamp") < last_time
    ):
        await update.message.reply_text(
            "⚠️ Устройство не ответило. Возможно, оно недоступно.",
            disable_notification=True,  # 🔕 бесшумно
        )
    # else:
    # await update.message.reply_text("✅ Устройство подтвердило выполнение команды.")


async def is_gate_available_for_user(
    user_id: str, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    last_user_id = context.bot_data.get("last_active_user_id")
    state = gate_state.get("current", "IDLE")
    log(f"[DEBUG] Текущее состояние калитки: {state}")

    if last_user_id and last_user_id != user_id and state != "IDLE":
        return False  # Калитка занята другим
    return True  # Всё в порядке


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


def on_disconnect(client, userdata, rc, properties):
    if rc != 0:
        log(f"[⚠️] MQTT отключился неожиданно (rc={rc}) — пытаемся переподключиться...")
        try:
            client.reconnect()
            log("[🔁] Попытка переподключения отправлена")
        except Exception as e:
            log(f"[❌] Ошибка при переподключении: {e}")
    else:
        log("[ℹ️] MQTT отключился по инициативе клиента (rc=0)")


def get_dynamic_keyboard(context, user_id=None):
    user_id = str(user_id)
    state = gate_state.get("current", "IDLE")
    last_user = str(context.bot_data.get("last_active_user_id"))
    # log(f"[📲] Кнопки запрошены: user_id={user_id}, last_user={last_user}")

    # Только активному пользователю отображаем динамическую клавиатуру
    if user_id != last_user:
        log(f"[🔒] Пользователь не является активным — кнопки не отображаются")
        return None

    if state == "IDLE":
        # log("[🎛️] Отдаем кнопку: 🚪 Открыть (IDLE)")
        return [["🚪 Открыть"]]
    elif state == "OPENING":
        # log("[🎛️] Отдаем кнопку: ⏹ Остановить")
        return [["⏹ Остановить"]]
    elif state == "STOPPED":
        # log("[🎛️] Отдаем кнопку: 🔒 Закрыть")
        return [["🔒 Закрыть"]]
    elif state == "CLOSING":
        # log("[🎛️] Отдаем кнопку: ⏹ Остановить")
        return [["⏹ Остановить"]]
    else:
        # log("[🎛️] Неизвестное состояние — кнопки не отдаем")
        return None


def on_mqtt_message(client, userdata, msg, properties=None):
    app = userdata["app"]
    context = userdata["context"]
    loop = app.bot_data.get("event_loop")

    try:
        payload_raw = msg.payload.decode()
        log(f"[MQTT] 📥 Получено сообщение: topic={msg.topic}, payload={payload_raw}")

        # Попытка распарсить JSON
        try:
            data = json.loads(payload_raw)
            payload = data.get("command") or data.get("status") or payload_raw
        except json.JSONDecodeError as e:
            log(f"[❌] Ошибка при разборе JSON payload: {e}")
            return

        log(f"[MQTT] Состояние: {payload}")

        user_id = data.get("user_id")
        username = data.get("username")
        if context and "status" in data and "timestamp" in data and user_id:
            process_gate_status(data, context)

        if user_id:
            log(
                f"[MQTT] Активный пользователь (из payload): {user_id}, username={username}"
            )
        else:
            log("[MQTT] Пользователь в payload не указан")

        # Обновление текущего состояния
        gate_state["current"] = payload

        # Формируем клавиатуру
        if payload == "IDLE":
            context.bot_data["last_active_user_id"] = None
            log("[🔁] Сброс активного пользователя (IDLE)")
            log("[🔁] Калитка перешла в режим ожидания")

            keyboard = get_main_menu(status="yes", dynamic_buttons=None)
            text = "🔒"

            future = asyncio.run_coroutine_threadsafe(
                app.bot.send_message(
                    chat_id=int(user_id),
                    text=text,
                    reply_markup=keyboard,
                    disable_notification=True,  # 🔕 бесшумно
                ),
                loop,
            )
            future.result(timeout=10)
            log(f"[✅] Замочек и меню отправлены для {user_id}, username={username}")
            return
        else:
            # Обновляем last_active_user_id
            if user_id:
                context.bot_data["last_active_user_id"] = user_id

            dynamic_buttons = get_dynamic_keyboard(context, user_id=user_id)
            keyboard = get_main_menu("yes", dynamic_buttons)

            text = {
                "OPENING": "🔓 Калитка начала открываться",
                "CLOSING": "🔒 Калитка начала закрываться",
                "STOPPED": "⏹ Калитка остановлена",
            }.get(payload)

            if not text:
                return  # ничего не отправлять

        future = asyncio.run_coroutine_threadsafe(
            app.bot.send_message(
                chat_id=user_id,
                text=text,
                reply_markup=keyboard,
                disable_notification=True,
            ),
            loop,
        )
        future.result(timeout=10)
        log(
            f"[✅] Сообщение отправлено Telegram пользователю {user_id}, username={username}"
        )

    except Exception as e:
        log(f"[❌] Ошибка в on_mqtt_message: {e}")


def init_mqtt(application, context):
    context.bot_data["last_active_user_id"] = None
    client_id = f"client_{random.randint(1, 100000)}"
    client = mqtt.Client(
        client_id=client_id,
        protocol=mqtt.MQTTv5,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )
    client.username_pw_set(username=MQTT_USER, password=MQTT_PASS)
    client.user_data_set({"app": application, "context": context})
    client.on_message = on_mqtt_message
    client.on_disconnect = on_disconnect

    try:
        client.connect(HOST, port=1883, keepalive=60)
    except Exception as e:
        log(f"[❌] Ошибка MQTT подключения: {e}")
        return

    client.subscribe("gate/status")
    log(f"[MQTT] Подписка вызвана (в init_mqtt) → client_id={client_id}")
    log("[MQTT] ✅ Подписка на gate/status выполнена")
    client.loop_start()


def send_gate_command(command: str, user_id: str, username: str) -> Optional[str]:
    if not MQTT_USER or not MQTT_PASS:
        log("❌ MQTT переменные окружения не заданы.")
        return False

    payload = {
        "command": command,
        "user_id": user_id,
        "username": username,
        # "timestamp": datetime.now(timezone.utc).isoformat()
        "timestamp": datetime.now(moscow).isoformat(),
    }

    try:
        publish.single(
            topic="gate/command",
            payload=json.dumps(payload),
            hostname=HOST,
            port=1883,
            auth={"username": MQTT_USER, "password": MQTT_PASS},
            qos=0,  # “Fire and forget” Не получит и не восстановится
            retain=False,  # Не сохранит последнее сообщение по теме не передаёт его после переподключения
        )
        log(f"[📤] MQTT: отправлено {payload}")
        return payload["timestamp"]
    except Exception as e:
        print(f"[❌] MQTT ошибка: {e}")
        return False


def get_access_time_for_user(user_id: str) -> Optional[str]:
    sheet = get_sheet()
    records = safe_get_all_records(sheet)
    for row in records:
        if str(row.get("user_id")) == user_id:
            return str(row.get("access_time", "")).strip().lower() or None
    return None  # не указан


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


def normalize_phone(phone):
    return re.sub(r"\D", "", str(phone))[-10:] if phone else ""


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


def get_main_menu(status: str = "none", dynamic_buttons=None):
    keyboard = []

    if status == "yes":
        if dynamic_buttons is not None and len(dynamic_buttons) > 0:
            keyboard.append(dynamic_buttons[0])
        else:
            keyboard.append(["🚪 Открыть"])  # fallback только на открытие

        keyboard.append(["🔁 Изменить номер"])
        keyboard.append(["ℹ️ Помощь", "🏁 Начало"])

    elif status == "no":
        keyboard = [["🔄 Проверить статус", "ℹ️ Помощь", "🏁 Начало"]]

    elif status == "pending":
        keyboard = [["🔄 Проверить статус", "🔁 Изменить номер", "ℹ️ Помощь", "🏁 Начало"]]

    else:
        keyboard = [
            ["📋 Зарегистрироваться"],
            ["🔄 Проверить статус", "ℹ️ Помощь", "🏁 Начало"],
        ]

    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


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

    # ✅ Сброс состояния пользователя
    context.user_data.clear()

    log(f"[🔄] Пользователь активен (start): {user_id}")

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
    # log("📲 Старт: выход из ConversationHandler")
    return ConversationHandler.END


async def handle_start_button(update, context):
    # log("🏁 Кнопка 'Начало' нажата")
    context.user_data.clear()
    return await start(update, context)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_reply(
        update.message,
        "ℹ️ Доступные команды:\n/start — начать\n📋 Зарегистрироваться\n🔁 Изменить номер\n🔄 Проверить статус\nℹ️ Помощь — информация об администраторе",
    )


async def help_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = str(user.id) if user else None

    if not user_id:
        return

    # Получаем текущий статус пользователя
    status = get_user_status(user_id)

    # Проверяем, активен ли пользователь по статусу И назначен ли он last_active
    last_user = str(context.bot_data.get("last_active_user_id"))
    is_active = status == "yes" and user_id == last_user

    # Формируем клавиатуру
    dynamic_buttons = (
        get_dynamic_keyboard(context, user_id=user_id) if is_active else None
    )
    keyboard = get_main_menu(status=status, dynamic_buttons=dynamic_buttons)

    await safe_reply(
        update.message or update.callback_query.message,
        "ℹ️По вопросам добавления обращайтесь к @SergeyIvanov1987\n🛠️ По всем техническим вопросам к @DanielPython",
        reply_markup=keyboard,
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
        [[button], ["🏁 Начало"]], resize_keyboard=True, one_time_keyboard=True
    )
    await safe_reply(
        update.message, "Теперь отправьте номер телефона:", reply_markup=keyboard
    )
    return ASK_PHONE


async def change_phone_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["change_mode"] = True
    button = KeyboardButton("📱 Отправить новый номер", request_contact=True)
    keyboard = ReplyKeyboardMarkup(
        [[button], ["🏁 Начало"]], resize_keyboard=True, one_time_keyboard=True
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
    elif text and re.fullmatch(r"(\+7|8)\d{10}", text.strip()):
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

    # ✅ Ответ пользователю
    await safe_reply(
        update.message,
        "✅ Заявка отправлена. Ожидайте одобрения.",
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

    if await is_too_soon(update, context):
        return

    if not await is_gate_access_granted(user_id, update):
        return
    else:
        log(f"[🔓] Разрешённый доступ: user_id={user_id}, username={user.username}")

    access_time = get_access_time_for_user(user_id)

    if not access_time or not check_access_time(access_time):
        log(
            f"[⏰] Попытка доступа к калитке вне времени: user_id={user_id},username={user.username} access_time={access_time}"
        )
        await safe_reply(
            update.message,
            "🕒 Доступ к калитке возможен только в разрешённое время.",
        )
        return
    else:
        log(f"[🔓] username={user.username} time is OK")

    log(f"[🆗] Назначен активный пользователь: {user_id}, username={user.username}")

    # ⛔ Проверка: если уже другой активный пользователь
    if not await is_gate_available_for_user(user_id, context):
        await update.message.reply_text(
            "🚫 Калитка сейчас используется другим пользователем."
        )
        return

    # 📤 Отправляем команду

    timestamp_str = send_gate_command("OPEN", user_id, username)
    if not timestamp_str:
        await update.message.reply_text("❌ Ошибка отправки команды.")
        return
    context.user_data["last_command_timestamp"] = isoparse(timestamp_str)
    await wait_for_arduino_confirmation(
        context=context, user_id=user_id, update=update, command_name="OPEN"
    )
    log(
        f"[🔓] Попытка открытия калитки по запросу: user_id={user.id}, username={user.username}"
    )

    # ⏳ Ответ пользователю (без клавиатуры)
    # await update.message.reply_text(
    #     "📤 Команда отправлена. Ожидаем подтверждение от калитки..."
    # )


async def stop_gate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = str(user.id)
    username = user.username or "unknown"

    if await is_too_soon(update, context):
        return

    if not await is_gate_access_granted(user_id, update):
        return

    access_time = get_access_time_for_user(user_id)

    if not access_time or not check_access_time(access_time):
        log(
            f"[⏰] Попытка доступа к калитке вне времени: user_id={user_id},username={user.username} access_time={access_time}"
        )
        await safe_reply(
            update.message,
            "🕒 Доступ к калитке возможен только в разрешённое время.",
        )
        return
    else:
        log(f"[🔓] username={user.username} time is OK")

    if not await is_gate_available_for_user(user_id, context):
        await update.message.reply_text(
            "🚫 Калитка сейчас используется другим пользователем."
        )
        return

    timestamp_str = send_gate_command("STOP", user_id, username)
    if not timestamp_str:
        await update.message.reply_text("❌ Ошибка отправки команды.")
        return
    context.user_data["last_command_timestamp"] = isoparse(timestamp_str)
    await wait_for_arduino_confirmation(
        context=context, user_id=user_id, update=update, command_name="STOP"
    )

    dynamic_buttons = get_dynamic_keyboard(context, user_id=user_id)
    keyboard = get_main_menu("yes", dynamic_buttons)


async def close_gate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = str(user.id)
    username = user.username or "unknown"

    if await is_too_soon(update, context):
        return

    if not await is_gate_access_granted(user_id, update):
        return

    access_time = get_access_time_for_user(user_id)

    if not access_time or not check_access_time(access_time):
        log(
            f"[⏰] Попытка доступа к калитке вне времени: user_id={user_id},username={user.username} access_time={access_time}"
        )
        await safe_reply(
            update.message,
            "🕒 Доступ к калитке возможен только в разрешённое время.",
        )
        return
    else:
        log(f"[🔓] username={user.username} time is OK")

    if not await is_gate_available_for_user(user_id, context):
        await update.message.reply_text(
            "🚫 Калитка сейчас используется другим пользователем."
        )
        return

    timestamp_str = send_gate_command("CLOSE", user_id, username)
    if not timestamp_str:
        await update.message.reply_text("❌ Ошибка отправки команды.")
        return
    context.user_data["last_command_timestamp"] = isoparse(timestamp_str)
    await wait_for_arduino_confirmation(
        context=context, user_id=user_id, update=update, command_name="STOP"
    )

    dynamic_buttons = get_dynamic_keyboard(context, user_id=user_id)
    keyboard = get_main_menu("yes", dynamic_buttons)


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


async def is_gate_access_granted(user_id: str, update: Update) -> bool:
    sheet = get_sheet()
    if not sheet:
        await update.message.reply_text("❌ Ошибка доступа к таблице.")
        return False

    records = safe_get_all_records(sheet)
    for row in records:
        if str(row.get("user_id")) == user_id:
            status = row.get("aprove", "").strip().lower()
            if status == "yes":
                return True
            elif status == "no":
                await update.message.reply_text("🚫 Ваш доступ был отклонён.")
                return False
            else:
                await update.message.reply_text("⏳ Ваша заявка ещё рассматривается.")
                return False

    await update.message.reply_text("🚫 Вы не зарегистрированы.")
    return False


async def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.bot_data["event_loop"] = asyncio.get_event_loop()

    conv_handler = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex("📋 Зарегистрироваться"), register_start),
            MessageHandler(filters.Regex("🔁 Изменить номер"), change_phone_start),
        ],
        states={
            ASK_NAME: [
                MessageHandler(filters.Regex("^🏁 Начало$"), handle_start_button),
                MessageHandler(filters.TEXT & ~filters.COMMAND, ask_name),
            ],
            ASK_PHONE: [
                MessageHandler(filters.Regex("^🏁 Начало$"), handle_start_button),
                MessageHandler(filters.CONTACT, ask_phone),
                MessageHandler(filters.TEXT & ~filters.COMMAND, ask_phone),
            ],
        },
        fallbacks=[
            MessageHandler(filters.Regex("^🏁 Начало$"), handle_start_button),
            CommandHandler("cancel", cancel),
        ],
    )

    app.add_handler(conv_handler)
    app.add_handler(CallbackQueryHandler(handle_admin_decision))
    app.add_handler(CommandHandler("myid", my_id))
    app.add_handler(MessageHandler(filters.Regex("🏁 Начало"), start))
    app.add_handler(
        MessageHandler(filters.Regex("🔄 Проверить статус"), check_status)
    )  # ⬅️ сюда
    app.add_handler(MessageHandler(filters.Regex("ℹ️ Помощь"), help_button))
    app.add_handler(MessageHandler(filters.Regex("🚪 Открыть"), open_gate))
    app.add_handler(MessageHandler(filters.Regex("⏹ Остановить"), stop_gate))
    app.add_handler(MessageHandler(filters.Regex("🔒 Закрыть"), close_gate))
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, unknown_input))

    if MODE == "webhook":
        print("🚀 Запуск в WEBHOOK режиме. Введите /start в Telegram.")

        PORT = int(os.getenv("PORT", 8443))
        webhook_url = f"https://{DOMAIN_IP}:{PORT}/bot{BOT_TOKEN}"

        init_mqtt(app, app)
        await app.bot.set_webhook(webhook_url)

        await app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            webhook_url=webhook_url,
            cert="certs/webhook.crt",
            key="certs/webhook.key",
            url_path=f"bot{BOT_TOKEN}",
        )
    else:
        log("🚀 Запуск в polling режиме. Введите /start в Telegram.")
        init_mqtt(app, app)
        await app.run_polling()


if __name__ == "__main__":
    import nest_asyncio

    nest_asyncio.apply()
    asyncio.get_event_loop().run_until_complete(main())
