import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import time
import random
from datetime import datetime
import logging
import sys
import os
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === 1️⃣ Настройки логгера ===
log_filename = "aisoip_log.txt"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_filename, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

# === 2️⃣ URL и заголовки ===
base_url_list = "https://aisoip.adilet.gov.kz/rest/debtor/findErd"
base_url_detail = "https://aisoip.adilet.gov.kz/rest/debtor/findErd/detail"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ru",
    "Referer": "https://aisoip.adilet.gov.kz/debtors"
}

# === 3️⃣ Сессия с retry ===
session = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=2,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["GET", "POST"]
)
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)

# === 4️⃣ Загрузка ИИНов ===
try:
    df_iin = pd.read_csv("divided_filtered_iin_part2_1.csv", header=None, names=["iin"])
    iins = df_iin["iin"].astype(str).tolist()
except Exception as e:
    logging.error(f"Ошибка при загрузке iin.csv: {e}")
    sys.exit(1)

if not iins:
    logging.warning("⚠️ Файл iin.csv пуст. Добавьте хотя бы один ИИН.")
    sys.exit(0)

logging.info(f"📥 Загружено ИИНов: {len(iins)}")

# === 5️⃣ Файл прогресса ===
progress_file = "progress2.txt"
processed_iins = set()

if os.path.exists(progress_file):
    try:
        with open(progress_file, "r", encoding="utf-8") as f:
            processed_iins = set(line.strip() for line in f if line.strip())
        logging.info(f"🔁 Найден файл прогресса — уже обработано {len(processed_iins)} ИИНов")
    except Exception as e:
        logging.warning(f"⚠️ Не удалось прочитать progress.txt: {e}")

def save_progress(iin):
    try:
        with open(progress_file, "a", encoding="utf-8") as f:
            f.write(str(iin) + "\n")
    except Exception as e:
        logging.error(f"❌ Ошибка при записи в progress.txt: {e}")

# === 6️⃣ Возраст по ИИН ===
def age_from_iin(iin: str) -> int:
    try:
        year = int(iin[0:2])
        month = int(iin[2:4])
        day = int(iin[4:6])
        century_code = int(iin[6])
        if century_code in [1, 2]:
            year += 1800
        elif century_code in [3, 4]:
            year += 1900
        elif century_code in [5, 6]:
            year += 2000
        else:
            return 0
        birth_date = datetime(year, month, day)
        today = datetime.now()
        return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
    except:
        return 0

# === 7️⃣ Основной код ===
output_file = "debtors_detailed_1.csv"
all_records = []

SAVE_EVERY = 50  # каждые 50 ИИН сохраняем (или когда накопилось много дел)
processed_iins_count = 0

for iin in iins:
    if iin in processed_iins:
        logging.info(f"⏭ Пропуск уже обработанного ИИН: {iin}")
        continue

    processed_iins_count += 1 
    logging.info(f"🔎 Обработка ИИН: {iin}")
    age = age_from_iin(iin)
    if age < 18:
        logging.info(f"🚫 Пропуск — гражданину {age} лет (<18).")
        save_progress(iin)
        continue

    params_list = {"page": 0, "size": -1}
    payload = {
        "iin": iin,
        "fullName": "",
        "searchType": 0,
        "captcha": "HFMXdsIg5KUTEVAkhZH11IHhkTMzw9QB4rTR8LYDweWhooRCY9VQJxYRdgdy1McAAqXxB1VQQfGEJTDDQnYQQRc3sRUVJ-GVtXKUomJEUzZXBKfSM1Cz1CPxkLLVJIEkJCCEl5bSZ7W0QDWxdtBV8eIX5zbTMKQXpRFnASUVhcA1tEUlwIQgQIQn8RJCtMbVpwFwoMETYdXhMtWSFbVnRkYxUrNwU",
        "action": "findErd"
    }

    blocked = False

    for attempt in range(3):
        try:
            resp = session.post(base_url_list, headers=headers, params=params_list, json=payload, timeout=25, verify=False)

            if resp.status_code in [403, 429]:
                logging.warning(f"🚫 Блокировка/ограничение ({resp.status_code}) — пауза 60 сек")
                blocked = True
                time.sleep(60)
                continue

            resp.raise_for_status()
            data = resp.json()
            debtors = data.get("content", [])
            searchid = data.get("pagination", {}).get("searchId")

            if not debtors:
                logging.info(f"🚫 По ИИН {iin} не найдено дел.")
                break

            logging.info(f"📋 Найдено дел: {len(debtors)}")

            for d in debtors:
                params_detail = {
                    "id": d["id"],
                    "typedata": d["typeData"],
                    "uuid": d["uid"],
                    "searchid": searchid
                }

                try:
                    r = session.get(base_url_detail, headers=headers, params=params_detail, timeout=20, verify=False)
                    if r.status_code in [403, 429]:
                        logging.warning("🚫 Блокировка при деталях — ждём 60 сек")
                        time.sleep(60)
                        blocked = True
                        continue

                    if r.status_code >= 500:
                        logging.warning(f"⚠️ Серверная ошибка {r.status_code}, ждём 10 сек")
                        time.sleep(10)
                        continue

                    if r.status_code == 200:
                        json_data = r.json()
                        detail = json_data.get("detailInfo") or {}
                        primary = json_data.get("primaryAccount") or {}
                        secondary = json_data.get("secondaryAccount") or {}

                        record = {
                            "ИИН": iin,
                            "Возраст": age,
                            "ФИО должника": detail.get("debtorFullName", ""),
                            "Орган, выдавший исполнительный документ": detail.get("ilOrgan_ru", ""),
                            "Исполнитель": detail.get("officerFullName", ""),
                            "Телефон": detail.get("officerPhone", ""),
                            "Email": detail.get("officerEmail", ""),
                            "Адрес исполнителя": detail.get("officerAddress", ""),
                            "Номер ИП": detail.get("execProcNum", ""),
                            "Дата начала ИП": detail.get("ipStartDate", ""),
                            "Взыскатель": detail.get("recovererFullName", ""),
                            "Сумма взыскания": detail.get("recoveryAmount", ""),
                            "Орган исполнительного пр-ва, судебный исполнитель": detail.get("disaDepartmentName_ru", ""),
                            "Статус": detail.get("status_ru", ""),
                            "Получатель (основной)": primary.get("name_ru", ""),
                            "ИИК (основной)": primary.get("iik", ""),
                            "БИК (основной)": primary.get("bik", ""),
                            "КБЕ (основной)": primary.get("kbe", ""),
                            "КНП (основной)": primary.get("knp", ""),
                            "КБК (основной)": primary.get("kbk", ""),
                            "КНО (основной)": primary.get("kno", ""),
                            "Получатель (второй)": secondary.get("name_ru", ""),
                            "ИИК (второй)": secondary.get("iik", ""),
                            "БИК (второй)": secondary.get("bik", ""),
                            "КБЕ (второй)": secondary.get("kbe", ""),
                            "КНП (второй)": secondary.get("knp", ""),
                            "КБК (второй)": secondary.get("kbk", ""),
                            "КНО (второй)": secondary.get("kno", ""),
                            "Дата запрета на выезд": detail.get("banStartDate", ""),
                            "id": d["id"],
                            "uuid": d["uid"]
                        }

                        all_records.append(record)
                        logging.info(f"✅ Детали по делу {d['id']} добавлены")
                    else:
                        logging.warning(f"⚠️ Ошибка {r.status_code} при деталях по id {d['id']}")
                except Exception as e2:
                    logging.error(f"❌ Ошибка при деталях по id {d.get('id')}: {e2}")

                time.sleep(random.uniform(0.05, 0.2))  # между делами

            break

        except requests.exceptions.SSLError as e:
            logging.warning(f"⚠️ SSL ошибка (попытка {attempt+1}/3): {e}")
            time.sleep(8)
        except requests.exceptions.RequestException as e:
            logging.warning(f"⚠️ Ошибка соединения (попытка {attempt+1}/3): {e}")
            time.sleep(5)
        except Exception as e:
            logging.error(f"❌ Неожиданная ошибка: {e}")
            time.sleep(5)

    # 💾 Сохраняем каждые N ИИН или если накопилось много записей
    if (processed_iins_count % SAVE_EVERY == 0) or len(all_records) > 200:
        try:
            df = pd.DataFrame(all_records)
            file_exists = os.path.exists(output_file)
            df.to_csv(
                output_file,
                mode='a',
                header=not file_exists,
                index=False,
                encoding='utf-8-sig'
            )
            logging.info(
                f"💾 Сохранено {len(all_records)} записей "
                f"(после {processed_iins_count} ИИН)"
            )
            all_records.clear()
        except Exception as e:
            logging.error(f"❌ Ошибка при сохранении файла: {e}")
    
    save_progress(iin)

    if blocked:
        logging.info("⏳ Отдых 60 сек после блокировки...")
        time.sleep(60)
    else:
        time.sleep(random.uniform(0.05, 0.15))

if all_records:
    df = pd.DataFrame(all_records)
    file_exists = os.path.exists(output_file)
    df.to_csv(
        output_file,
        mode='a',
        header=not file_exists,
        index=False,
        encoding='utf-8-sig'
    )
    logging.info(f"💾 Финальное добавление {len(all_records)} записей")

logging.info("✅ Работа завершена полностью!")
logging.info(f"📂 Итоговый файл: {output_file}")
logging.info(f"🪵 Логи сохранены в: {log_filename}")
logging.info(f"📘 Прогресс сохранён в: {progress_file}")
