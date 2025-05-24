import requests
import time
from datetime import datetime, timedelta
import pytz
import pandas as pd
from sqlalchemy import create_engine, Column, String, Integer, TIMESTAMP, ForeignKey, Table, MetaData, insert, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker


WAIT = 1  # Tempo de espera entre as requisições (em minutos)

# 🔑 Configuração do banco PostgreSQL (exemplo Railway)
DATABASE_URL = 'postgresql://postgres:cLgkdZzJkpllnKxHvNniMgbKHVEgLeMC@postgres.railway.internal:5432/railway'  # coloque seus dados aqui

# 🎥 Lista dos vídeos que você quer monitorar
VIDEOS = [
    {'video_id': '-4GmbBoYQjE', 'titulo': 'I Explored 2000 Year Old Ancient Temples'}
]

# 🔑 API KEY do YouTube
API_KEY = 'AIzaSyACx1i4XGXJjRvQJukTTvZCvD6FNexhgmg'


# Telegram bot config
TELEGRAM_BOT_TOKEN = '8134403690:AAHU8PXdmMKI2Ag_kXHjRnO0ZpKvJlOejwQ'  # coloque o token do seu bot
TELEGRAM_CHAT_ID = '5423161617'  # coloque o chat id do destinatário

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

try:
    while True:
        agora_brasilia = datetime.now(brasilia_tz).replace(second=0, microsecond=0)

        with engine.begin() as conn:
            for video in VIDEOS:
                video_id = video['video_id']
                views = get_video_stats(video_id, API_KEY)

                if views is not None:
                    print(f'[{agora_brasilia.strftime("%Y-%m-%d %H:%M:%S")}] {video_id}: {views} views')

                    stmt = pg_insert(views_table).values(
                        video_id=video_id,
                        horario=agora_brasilia,
                        views=views
                    ).on_conflict_do_nothing()

                    conn.execute(stmt)

                    # Envia mensagem para Telegram
                    mensagem = (
                        f"📊 Atualização de views:\n"
                        f"Vídeo: <b>{video['titulo']}</b>\n"
                        f"ID: {video_id}\n"
                        f"Views: <b>{views}</b>\n"
                        f"Horário (BR): {agora_brasilia.strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    send_telegram_message(mensagem)
                else:
                    print(f'Não conseguiu obter views para {video_id}')

        # 👉 Espera até o próximo múltiplo de WAIT minutos
        time.sleep(wait_time())

except KeyboardInterrupt:
    print("Parado pelo usuário.")
