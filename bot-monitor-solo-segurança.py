"""
==================================================================================
PROJETO: MONITOR DE DEFESA CIVIL COMUNITÁRIO — v3.3 (Clusterização e LGPD)
AUTOR ORIGINAL: João Gabriel (Checo)
REVISÃO TÉCNICA: Engenheiro de Software Sênior

NOVIDADES v3.3:
  [1] Performance: Clusterização geográfica no motor de alertas (O(1) req/cidade).
  [2] Compliance: LGPD integrada ao texto de onboarding.
  [3] Segurança: Token estrito via variável de ambiente (.env).
==================================================================================
"""

import logging
import logging.handlers
import os
import shelve
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from math import radians, sin, cos, sqrt, atan2
from typing import Optional, Tuple

import requests
import telebot
from telebot import types
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from geopy.geocoders import Nominatim
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from dotenv import load_dotenv

# ══════════════════════════════════════════════════════════════════════════════
# CAMADA 1 — CONFIGURAÇÃO CENTRALIZADA
# ══════════════════════════════════════════════════════════════════════════════

load_dotenv()

class Config:
    TELEGRAM_TOKEN: str = os.getenv("TELEGRAM_TOKEN")
    DB_PATH: str = "usuarios_bot.db"
    GEOCODE_CACHE_PATH: str = "geocode_cache"
    INTERVALO_MONITORAMENTO: int = 60

    LIMIAR_CRITICO_ACUMULADO_MM: float = 60.0
    LIMIAR_CRITICO_SOLO_PCT: float = 85.0
    LIMIAR_CRITICO_SENSOR_MM: float = 25.0
    LIMIAR_LARANJA_PREVISAO_MM: float = 20.0
    LIMIAR_LARANJA_CHUVA_ATUAL: float = 10.0
    LIMIAR_LARANJA_SENSOR_MM: float = 15.0
    LIMIAR_AMARELO_ACUMULADO_MM: float = 60.0
    LIMIAR_AMARELO_SOLO_PCT: float = 80.0

    INMET_RAIO_MAXIMO_KM: float = 50.0
    INMET_API_BASE: str = "https://apitempo.inmet.gov.br"

    LOG_PATH: str = "defesa_civil.log"
    LOG_MAX_BYTES: int = 5 * 1024 * 1024
    LOG_BACKUP_COUNT: int = 3

# ══════════════════════════════════════════════════════════════════════════════
# CAMADA 2 — LOGGING E BANCO DE DADOS
# ══════════════════════════════════════════════════════════════════════════════

def configurar_logging() -> logging.Logger:
    logger = logging.getLogger("DefesaCivil")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(funcName)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)
    arquivo = logging.handlers.RotatingFileHandler(Config.LOG_PATH, maxBytes=Config.LOG_MAX_BYTES, backupCount=Config.LOG_BACKUP_COUNT, encoding="utf-8")
    arquivo.setLevel(logging.DEBUG)
    arquivo.setFormatter(fmt)
    logger.addHandler(console)
    logger.addHandler(arquivo)
    return logger

logger = configurar_logging()

@contextmanager
def get_db():
    conn = sqlite3.connect(Config.DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def iniciar_banco() -> None:
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                chat_id        INTEGER PRIMARY KEY,
                cidade         TEXT    NOT NULL,
                lat            REAL    NOT NULL,
                lon            REAL    NOT NULL,
                memoria_alerta INTEGER NOT NULL DEFAULT 1,
                cadastrado_em  TEXT    NOT NULL DEFAULT (datetime('now'))
            )
        """)
        cols = [r[1] for r in conn.execute("PRAGMA table_info(usuarios)").fetchall()]
        if "uf" not in cols:
            conn.execute("ALTER TABLE usuarios ADD COLUMN uf TEXT DEFAULT ''")
    logger.info("Banco de dados inicializado.")

# ══════════════════════════════════════════════════════════════════════════════
# CAMADA 3 — GEOCODING E API CLIMÁTICA
# ══════════════════════════════════════════════════════════════════════════════

geolocator = Nominatim(user_agent="defesa_civil_comunitaria_bot_v3_portfolio")

def geocodificar(busca: str) -> Optional[object]:
    chave = busca.lower().strip()
    with shelve.open(Config.GEOCODE_CACHE_PATH) as cache:
        if chave in cache:
            return cache[chave]
    try:
        time.sleep(1.1)
        local = geolocator.geocode(busca, addressdetails=True, timeout=10)
        if local:
            with shelve.open(Config.GEOCODE_CACHE_PATH) as cache:
                cache[chave] = local
        return local
    except Exception as e:
        logger.error(f"Geocoding falhou para '{busca}': {e}")
        return None

DadosClimaticos = Tuple[float, float, float, Optional[float], float]

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8), retry=retry_if_exception_type((requests.RequestException, KeyError)), reraise=True)
def _fetch_open_meteo(lat: float, lon: float) -> dict:
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=precipitation,soil_moisture_0_to_7cm&hourly=precipitation,precipitation_probability&timezone=America%2FSao_Paulo&past_days=3"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()

def obter_dados_climaticos(lat: float, lon: float) -> Optional[DadosClimaticos]:
    try:
        res = _fetch_open_meteo(lat, lon)
        chuva_atual = res.get("current", {}).get("precipitation", 0.0)
        umidade_raw = res.get("current", {}).get("soil_moisture_0_to_7cm")
        porcentagem_solo = (umidade_raw * 100) if umidade_raw else None
        df = pd.DataFrame({
            "hora": pd.to_datetime(res["hourly"]["time"]),
            "chuva": res["hourly"]["precipitation"],
            "prob": res["hourly"].get("precipitation_probability", [0] * len(res["hourly"]["time"]))
        })
        agora_sp = datetime.now(timezone(timedelta(hours=-3))).replace(tzinfo=None)
        agora = pd.to_datetime(agora_sp.strftime("%Y-%m-%d %H:00:00"))
        acumulado_72h = df[df["hora"] <= agora].tail(72)["chuva"].sum()
        previsao_3h   = df[df["hora"] > agora].head(3)["chuva"].sum()
        chance_chuva  = df[df["hora"] > agora].head(3)["prob"].max()
        return chuva_atual, acumulado_72h, previsao_3h, porcentagem_solo, chance_chuva
    except Exception as e:
        logger.error(f"Open-Meteo: falha para ({lat}, {lon}): {e}")
        return None

# ══════════════════════════════════════════════════════════════════════════════
# CAMADA 4 — SENSOR FÍSICO (INMET) E LÓGICA DE RISCO
# ══════════════════════════════════════════════════════════════════════════════

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat, dlon = radians(lat2 - lat1), radians(lon2 - lon1)
    a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))

@retry(stop=stop_after_attempt(2), wait=wait_exponential(min=2, max=6), retry=retry_if_exception_type(requests.RequestException), reraise=True)
def _get_inmet(endpoint: str) -> list:
    resp = requests.get(f"{Config.INMET_API_BASE}{endpoint}", timeout=10)
    resp.raise_for_status()
    return resp.json()

_inmet_estacoes_cache: dict = {"dados": None, "expira_em": 0.0}
_INMET_CACHE_TTL_SEGUNDOS: int = 6 * 3600

def _obter_estacoes_inmet() -> list:
    agora = time.monotonic()
    if _inmet_estacoes_cache["dados"] is not None and agora < _inmet_estacoes_cache["expira_em"]:
        return _inmet_estacoes_cache["dados"]
    estacoes = _get_inmet("/estacoes/T")
    _inmet_estacoes_cache["dados"] = estacoes
    _inmet_estacoes_cache["expira_em"] = agora + _INMET_CACHE_TTL_SEGUNDOS
    return estacoes

def busca_sensor_fisico(lat: float, lon: float, uf: str) -> Optional[float]:
    if not uf:
        return None
    try:
        estacoes = _obter_estacoes_inmet()
        mais_proxima, menor_dist = None, float("inf")
        for est in estacoes:
            lat_e = float(est.get("VL_LATITUDE") or est.get("lat") or 0)
            lon_e = float(est.get("VL_LONGITUDE") or est.get("lon") or 0)
            if lat_e == 0 and lon_e == 0: continue
            dist = _haversine_km(lat, lon, lat_e, lon_e)
            if dist < menor_dist:
                menor_dist, mais_proxima = dist, {**est, "_distancia_km": dist}
        if not mais_proxima or menor_dist > Config.INMET_RAIO_MAXIMO_KM: return None
        
        codigo = mais_proxima.get("CD_ESTACAO") or mais_proxima.get("codigo")
        hoje = datetime.now(timezone(timedelta(hours=-3))).strftime("%Y-%m-%d")
        dados = _get_inmet(f"/estacao/{hoje}/{hoje}/{codigo}")
        if not dados: return None
        chuva_raw = dados[-1].get("CHUVA") or dados[-1].get("chuva")
        return float(chuva_raw) if chuva_raw not in (None, "") else None
    except Exception as e:
        logger.warning(f"INMET: falha para {uf}: {e}")
        return None

def calcular_nivel_risco(chuva_atual: float, acumulado_72h: float, previsao_3h: float, solo: Optional[float], chuva_sensor: Optional[float] = None) -> Tuple[int, str]:
    cfg = Config
    sensor_ok = chuva_sensor is not None
    fonte = "📡 Satélite + 🌧️ Sensor Físico" if sensor_ok else "📡 Satélite"
    txt_sensor = f"\n• *Sensor (INMET):* {chuva_sensor:.1f} mm/h" if sensor_ok else ""

    if (acumulado_72h > cfg.LIMIAR_CRITICO_ACUMULADO_MM or (solo and solo > cfg.LIMIAR_CRITICO_SOLO_PCT)) and previsao_3h > 15:
        if sensor_ok and chuva_sensor >= cfg.LIMIAR_CRITICO_SENSOR_MM:
            return 4, f"🔴 *ALERTA CRÍTICO* ({fonte})\nSolo instável e chuva intensa. *Risco iminente!* Afaste-se de encostas.{txt_sensor}"
        elif sensor_ok:
            return 3, f"🟠 *ALERTA LARANJA* ({fonte})\nSatélite indica risco, mas sensor mede chuva moderada. Mantenha atenção.{txt_sensor}"
        else:
            return 4, f"🔴 *ALERTA CRÍTICO* ({fonte})\nSolo instável e tempestade prevista. *Risco iminente!* Afaste-se de encostas."
    if previsao_3h > cfg.LIMIAR_LARANJA_PREVISAO_MM or chuva_atual > cfg.LIMIAR_LARANJA_CHUVA_ATUAL or (sensor_ok and chuva_sensor >= cfg.LIMIAR_LARANJA_SENSOR_MM):
        return 3, f"🟠 *ALERTA LARANJA* ({fonte})\nChuva intensa. Risco de alagamentos.{txt_sensor}"
    if acumulado_72h > cfg.LIMIAR_AMARELO_ACUMULADO_MM or (solo and solo > cfg.LIMIAR_AMARELO_SOLO_PCT):
        return 2, f"🟡 *ATENÇÃO* ({fonte})\nSolo saturado. Qualquer chuva aumenta o risco.{txt_sensor}"
    return 1, f"✅ *ESTABILIDADE* ({fonte})\nRisco baixo. Solo em drenagem.{txt_sensor}"

# ══════════════════════════════════════════════════════════════════════════════
# CAMADA 5 — HANDLERS, MENUS E MOTOR DE ALERTAS
# ══════════════════════════════════════════════════════════════════════════════

def _menu_principal():
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(
        types.InlineKeyboardButton("📊 Status Agora", callback_data="clima"),
        types.InlineKeyboardButton("📍 Atualizar Endereço", callback_data="atualizar"),
        types.InlineKeyboardButton("❌ Sair / Parar Alertas", callback_data="sair")
    )
    return markup

def _menu_confirmacao_sair():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("✅ Sim, remover", callback_data="sair_confirma"),
        types.InlineKeyboardButton("❌ Cancelar", callback_data="menu")
    )
    return markup

def _iniciar_cadastro(chat_id: int, bot: telebot.TeleBot) -> None:
    texto = (
        "👋 *Olá! Sou o Monitor de Defesa Civil Comunitário.* 🛡️\n\n"
        "Eu estou aqui para ajudar a proteger você e sua família de tempestades, enchentes e deslizamentos na sua região.\n\n"
        "📡 *Como funciona?*\n"
        "Monitoramos 24h por dia os satélites de clima e os pluviômetros oficiais do governo. Se o solo da sua rua ficar perigoso ou uma tempestade extrema se aproximar, eu te mando um alerta automático para você se prevenir.\n\n"
        "🔒 *Privacidade:* Sua localização será salva exclusivamente para o cálculo de risco climático. Você pode apagar seu cadastro e seus dados a qualquer momento no menu.\n\n"
        "🗺️ *Para começarmos, qual é o seu Estado?*\n"
        "_(Ex: São Paulo, Minas Gerais)_"
    )
    resposta = bot.send_message(chat_id, texto, parse_mode="Markdown")
    bot.register_next_step_handler(resposta, _capturar_cidade, bot)

def _capturar_cidade(msg: types.Message, bot: telebot.TeleBot) -> None:
    estado = msg.text.strip()
    if not estado or len(estado) < 2:
        bot.send_message(msg.chat.id, "❌ Estado inválido. Use /start para tentar novamente.")
        return
    resposta = bot.send_message(msg.chat.id, "🏙️ Agora, *qual é a sua Cidade?*\n_(Ex: Guarulhos, Campinas)_", parse_mode="Markdown")
    bot.register_next_step_handler(resposta, _cadastrar_local, bot, estado)

def _cadastrar_local(msg: types.Message, bot: telebot.TeleBot, estado: str) -> None:
    chat_id = msg.chat.id
    busca = f"{msg.text.strip()}, {estado}, Brasil"
    status = bot.send_message(chat_id, "⏳ Localizando e analisando a região...")

    local = geocodificar(busca)
    if not local:
        bot.edit_message_text("❌ Cidade não encontrada. Tente /start novamente.", chat_id, status.message_id)
        return

    detalhes = local.raw.get("address", {})
    cidade = detalhes.get("city") or detalhes.get("town") or detalhes.get("village") or msg.text.strip()
    uf = detalhes.get("ISO3166-2-lvl4", "").replace("BR-", "").strip() or ""
    local_nome = f"{cidade} - {detalhes.get('state', estado)}"

    with get_db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO usuarios (chat_id, cidade, lat, lon, memoria_alerta, uf) VALUES (?, ?, ?, ?, 1, ?)",
            (chat_id, local_nome, local.latitude, local.longitude, uf)
        )

    bot.edit_message_text(f"✅ *Localização salva!* Buscando dados...", chat_id, status.message_id, parse_mode="Markdown")
    _exibir_clima(chat_id, bot, status.message_id)

def _exibir_clima(chat_id: int, bot: telebot.TeleBot, message_id: Optional[int] = None) -> None:
    with get_db() as conn:
        user = conn.execute("SELECT cidade, lat, lon, uf FROM usuarios WHERE chat_id = ?", (chat_id,)).fetchone()

    if not user:
        bot.send_message(chat_id, "❌ Você ainda não tem cadastro.", reply_markup=_menu_principal())
        return

    dados = obter_dados_climaticos(user["lat"], user["lon"])
    if not dados:
        txt = "⚠️ Dados de satélite indisponíveis agora. Tente novamente em alguns minutos."
        if message_id:
            bot.edit_message_text(txt, chat_id, message_id)
        else:
            bot.send_message(chat_id, txt, reply_markup=_menu_principal())
        return

    c_now, ac_72h, p_3h, solo, prob = dados
    chuva_sensor = busca_sensor_fisico(user["lat"], user["lon"], user["uf"]) if user["uf"] else None
    nivel, _ = calcular_nivel_risco(c_now, ac_72h, p_3h, solo, chuva_sensor)
    txt_solo = f"{solo:.1f}%" if solo is not None else "Sem dados"
    status_icons = {1: "🟢 Seguro", 2: "🟡 Atenção", 3: "🟠 Alerta", 4: "🔴 Crítico"}

    txt_sensor_str = f"\n• *Sensor Físico (INMET):* {chuva_sensor:.1f} mm/h ✅" if chuva_sensor is not None else ""
    texto = (f"📊 *Relatório Climático*\n📍 {user['cidade']}\n🕐 {datetime.now().strftime('%d/%m/%Y %H:%M')}\n\n"
             f"• *Chuva agora:* {c_now:.1f} mm/h\n• *Probabilidade (3h):* {prob:.0f}%\n"
             f"• *Acumulado (72h):* {ac_72h:.1f} mm\n• *Umidade do Solo:* {txt_solo}{txt_sensor_str}\n\n"
             f"*Status atual:* {status_icons.get(nivel, '—')}")

    if message_id:
        try:
            bot.edit_message_text(texto, chat_id, message_id, parse_mode="Markdown", reply_markup=_menu_principal())
        except Exception:
            bot.send_message(chat_id, texto, parse_mode="Markdown", reply_markup=_menu_principal())
    else:
        bot.send_message(chat_id, texto, parse_mode="Markdown", reply_markup=_menu_principal())

def ciclo_de_alertas(bot: telebot.TeleBot) -> None:
    logger.info("Ciclo de alertas iniciado (Modo Clusterizado).")
    
    with get_db() as conn:
        # Recupera apenas as cidades únicas (Otimização I/O)
        locais_unicos = conn.execute("SELECT DISTINCT cidade, lat, lon, uf FROM usuarios").fetchall()

    if not locais_unicos:
        return

    for local in locais_unicos:
        cidade = local["cidade"]
        try:
            dados_sat = obter_dados_climaticos(local["lat"], local["lon"])
            if dados_sat is None: 
                continue
            
            chuva_atual, acumulado_72h, previsao_3h, solo, _ = dados_sat
            chuva_sensor = busca_sensor_fisico(local["lat"], local["lon"], local["uf"]) if local["uf"] else None
            novo_nivel, mensagem = calcular_nivel_risco(chuva_atual, acumulado_72h, previsao_3h, solo, chuva_sensor)
            
            with get_db() as conn:
                # Busca todos os usuários associados a esta cidade específica
                usuarios_afetados = conn.execute(
                    "SELECT chat_id, memoria_alerta FROM usuarios WHERE cidade = ?", (cidade,)
                ).fetchall()
                
                # Executa disparo em massa (Broadcast)
                for user in usuarios_afetados:
                    chat_id = user["chat_id"]
                    if novo_nivel != user["memoria_alerta"]:
                        try:
                            bot.send_message(chat_id, mensagem, parse_mode="Markdown")
                            conn.execute("UPDATE usuarios SET memoria_alerta = ? WHERE chat_id = ?", (novo_nivel, chat_id))
                            time.sleep(0.05)
                        except Exception as e:
                            logger.error(f"Erro envio chat_id={chat_id}: {e}")
        except Exception as e:
            logger.error(f"Erro ciclo na cidade={cidade}: {e}")

def registrar_handlers(bot: telebot.TeleBot) -> None:
    @bot.message_handler(commands=["start", "ajuda"])
    def boas_vindas(msg: types.Message) -> None:
        with get_db() as conn:
            user = conn.execute("SELECT cidade FROM usuarios WHERE chat_id = ?", (msg.chat.id,)).fetchone()
        if user:
            bot.send_message(msg.chat.id, f"👋 Bem-vindo de volta!\n📍 Monitorando: *{user['cidade']}*\n\nO que você quer fazer?", parse_mode="Markdown", reply_markup=_menu_principal())
        else:
            _iniciar_cadastro(msg.chat.id, bot)

    @bot.callback_query_handler(func=lambda call: True)
    def callback_handler(call: types.CallbackQuery) -> None:
        chat_id = call.message.chat.id
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
            
        if call.data == "menu": bot.send_message(chat_id, "📋 Menu principal:", reply_markup=_menu_principal())
        elif call.data == "clima": _exibir_clima(chat_id, bot, call.message.message_id)
        elif call.data == "atualizar": _iniciar_cadastro(chat_id, bot)
        elif call.data == "sair": bot.send_message(chat_id, "⚠️ Remover seu cadastro?", reply_markup=_menu_confirmacao_sair())
        elif call.data == "sair_confirma":
            with get_db() as conn:
                deleted = conn.execute("DELETE FROM usuarios WHERE chat_id = ?", (chat_id,)).rowcount
            if deleted: bot.send_message(chat_id, "✅ Cadastro removido.")

    @bot.message_handler(commands=["menu"])
    def comando_menu(msg: types.Message) -> None:
        bot.send_message(msg.chat.id, "📋 Menu principal:", reply_markup=_menu_principal())
        
    @bot.message_handler(func=lambda msg: True)
    def fallback_geral(msg: types.Message) -> None:
        boas_vindas(msg)

# ══════════════════════════════════════════════════════════════════════════════
# CAMADA 6 — PONTO DE ENTRADA
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if not Config.TELEGRAM_TOKEN:
        logger.critical("TELEGRAM_TOKEN não definido! Crie o arquivo .env contendo TELEGRAM_TOKEN=seu_token")
        raise SystemExit(1)
    
    iniciar_banco()
    bot = telebot.TeleBot(Config.TELEGRAM_TOKEN, parse_mode=None)
    registrar_handlers(bot)

    scheduler = BackgroundScheduler(timezone="America/Sao_Paulo")
    scheduler.add_job(ciclo_de_alertas, trigger="interval", seconds=Config.INTERVALO_MONITORAMENTO, args=[bot], max_instances=1, misfire_grace_time=30, id="monitor_alertas")
    scheduler.start()
    
    logger.info("Bot aguardando mensagens... (Ctrl+C para parar)")
    try:
        bot.infinity_polling(allowed_updates=["message", "callback_query"], logger_level=logging.WARNING)
    except KeyboardInterrupt:
        logger.info("Interrupção recebida.")
    finally:
        scheduler.shutdown(wait=False)