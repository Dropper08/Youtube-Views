import requests
import time
from datetime import datetime, timedelta
import pytz
import pandas as pd
import os
from sqlalchemy import create_engine, Column, String, Integer, TIMESTAMP, ForeignKey, Table, MetaData, insert, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker


WAIT = 5  # Tempo de espera entre as requisições (em minutos)
VIEWS = 31654582

# 🔑 Configuração do banco PostgreSQL (exemplo Railway)
DATABASE_URL = API_KEY = os.getenv("DATABASE_URL")  # coloque seus dados aqui

# 🎥 Lista dos vídeos que você quer monitorar
VIDEOS = [
    {'video_id': '7ilQ2_3PspI', 'titulo': 'Sidemen'}
]

# 🔑 API KEY do YouTube
# API_KEY = 'AIzaSyACx1i4XGXJjRvQJukTTvZCvD6FNexhgmg'
API_KEY = os.getenv("API_KEY")


# Telegram bot config
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")  # coloque o token do seu bot
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # coloque o chat id do destinatário

def send_telegram_message(message: str):
    url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'HTML'
    }
    try:
        resp = requests.post(url, data=payload)
        if resp.status_code != 200:
            print(f'Erro ao enviar mensagem no Telegram: {resp.status_code} - {resp.text}')
    except Exception as e:
        print(f'Exception ao enviar mensagem no Telegram: {e}')

def now_brasilia():
    return datetime.now(pytz.timezone('America/Sao_Paulo'))

def wait_time():
    agora = now_brasilia()

    minutos_atuais = agora.minute
    minutos_proximo_bloco = ((minutos_atuais // WAIT) + 1) * WAIT

    if minutos_proximo_bloco == 60:
        proximo_bloco = (agora.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    else:
        proximo_bloco = agora.replace(minute=minutos_proximo_bloco, second=0, microsecond=0)

    espera_segundos = (proximo_bloco - agora).total_seconds()

    print(f"Aguardando {espera_segundos:.1f} segundos para começar em {proximo_bloco.strftime('%H:%M:%S')}")
    return espera_segundos

# 🚀 Conexão com o banco
engine = create_engine(DATABASE_URL)
metadata = MetaData()

# 🏗️ Definindo as tabelas
videos_table = Table(
    'videos', metadata,
    Column('video_id', String, primary_key=True),
    Column('titulo', String, nullable=False),
    Column('criado_em', TIMESTAMP(timezone=True), nullable=False, default=now_brasilia)
)

views_table = Table(
    'views', metadata,
    Column('video_id', String, ForeignKey('videos.video_id', ondelete="CASCADE"), primary_key=True),
    Column('horario', TIMESTAMP(timezone=True), primary_key=True, default=now_brasilia),
    Column('views', Integer, nullable=False)
)

# 🔧 Cria as tabelas se não existirem
metadata.create_all(engine)

# Sessão
Session = sessionmaker(bind=engine)
session = Session()

# 📡 Função para pegar as views
def get_video_stats(video_id, api_key):
    url = (
        f'https://www.googleapis.com/youtube/v3/videos'
        f'?part=statistics&id={video_id}&key={api_key}'
    )
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if 'items' in data and len(data['items']) > 0:
            stats = data['items'][0]['statistics']
            view_count = stats.get('viewCount')
            return int(view_count)
        else:
            print("Nenhum dado encontrado para este vídeo.")
            return None
    else:
        print(f'Erro {response.status_code}: {response.text}')
        return None

# ✅ Insere os vídeos na tabela se não existirem
with engine.begin() as conn:
    for video in VIDEOS:
        stmt = pg_insert(videos_table).values(
            video_id=video['video_id'],
            titulo=video['titulo']
        ).on_conflict_do_nothing()
        conn.execute(stmt)

# ⏰ Alinha para o próximo múltiplo de WAIT minutos
brasilia_tz = pytz.timezone('America/Sao_Paulo')
time.sleep(wait_time())


df = pd.read_csv("Dados_finais_corrigidos.csv")
# Converte a coluna 'horario' para datetime sem timezone
df["horario"] = pd.to_datetime(df["horario"]).dt.tz_localize(None)

try:
    while True:
        agora_brasilia = datetime.now(brasilia_tz).replace(second=0, microsecond=0)

        with engine.begin() as conn:
            for video in VIDEOS:
                video_id = video['video_id']
                views = get_video_stats(video_id, API_KEY)
                horario_atual = datetime.now(brasilia_tz).replace(second=0, microsecond=0)
                horario_atual = (horario_atual - timedelta(hours=4)).time()
                try:
                    # Tentar encontrar o horário e pegar as views
                    views_antigo = df[df["horario"].dt.time == horario_atual]["views"].iloc[0]
                except IndexError:
                    # Caso nenhuma linha seja encontrada
                    views_antigo = 0
                    print(f"Nenhum dado encontrado para o horário {horario_atual}")

                if views is not None:
                    # this_hour = conn.execute(
                    #     text("""
                    #         SELECT views FROM views
                    #         WHERE horario = CASE
                    #             WHEN date_trunc('hour', now()) = now()
                    #                 THEN date_trunc('hour', now() - interval '1 hour')
                    #             ELSE date_trunc('hour', now())
                    #         END;
                    #     """)
                    # ).scalar()
                    # print(now())
                    
                    # Buscar última entrada para esse vídeo
                    last_two_rows= conn.execute(
                        text("""
                            SELECT views, horario FROM views
                            WHERE video_id = :video_id
                            ORDER BY horario DESC
                            LIMIT 2
                        """), {'video_id': video_id}
                    ).fetchall()
                    
                    views_diff = 0
                    delta = 0
                    pace_per_hour = 0
                    pace_24h = 0
                    
                    if len(last_two_rows) == 2:
                        current_views, current_time = last_two_rows[0]
                        previous_views, previous_time = last_two_rows[1]

                        views_diff = views - current_views
                        # pace_per_24hour = (views_diff / WAIT) * 60 * 24
                        pace_per_hour = (views_diff / WAIT) * 60
                        delta = 100
                        if (current_views != previous_views):
                            # print(f'Current Views: {current_views}, Previous Views {previous_views}, Views Diff {views_diff}')
                            delta = (views_diff / (current_views - previous_views)) - 1


                    print(f'[{agora_brasilia.strftime("%Y-%m-%d %H:%M:%S")}] {video_id}: {views} views ({views_diff} desde a última atualização)')

                    stmt = pg_insert(views_table).values(
                        video_id=video_id,
                        horario=agora_brasilia,
                        views=views
                    ).on_conflict_do_nothing()

                    conn.execute(stmt)
                    if (views_antigo != 0):
                        previsao = int(views/(views_antigo/VIEWS))
                    else:
                        previsao = 0
                        
                    mensagem = (
                        f"📊 Atualização de views ({agora_brasilia.strftime('%Y-%m-%d %H:%M:%S')}):\n"
                        f"Vídeo: <b>{video['titulo']}</b>\n"
                        # f"Views: <b>{views} -> {previsao}</b>\n"
                        f"Views: <b>{views}</b>\n"
                        # f"View Video Antigo: <b>{views_antigo} -> {VIEWS}</b>\n"
                        f"Ultimos 5 minutos: <b>{views_diff} ({delta:.2%})</b>\n"
                        f"Pace estimado para 1h: <b>{int(pace_per_hour)}</b> views\n"
                        # f"Pace estimado para 24h: <b>{int(pace_per_24hour)}</b> views\n"
                        # f"Views nessa hora: <b>{views - this_hour}</b>"
                    )
                    send_telegram_message(mensagem)

                else:
                    print(f'Não conseguiu obter views para {video_id}')

        time.sleep(wait_time())

except KeyboardInterrupt:
    print("Parado pelo usuário.")
