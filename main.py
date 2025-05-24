import requests
import time
from datetime import datetime, timedelta
import pytz
import pandas as pd
from sqlalchemy import create_engine, Column, String, Integer, TIMESTAMP, ForeignKey, Table, MetaData, insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker


WAIT = 1  # Tempo de espera entre as requisições (em minutos)

# 🔑 Configuração do banco PostgreSQL (exemplo Railway)
DATABASE_URL = 'postgresql://postgres:DqVPbefCrJJneICVKwPTOUozzSmUjusn@postgres.railway.internal:5432/railway'  # coloque seus dados aqui

# 🎥 Lista dos vídeos que você quer monitorar
VIDEOS = [
    {'video_id': '-4GmbBoYQjE', 'titulo': 'I Explored 2000 Year Old Ancient Temples'}
]

# 🔑 API KEY do YouTube
API_KEY = 'AIzaSyACx1i4XGXJjRvQJukTTvZCvD6FNexhgmg'

def now_brasilia():
    return datetime.now(pytz.timezone('America/Sao_Paulo'))

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

# ⏰ Alinha para o próximo múltiplo de 5 minutos
brasilia_tz = pytz.timezone('America/Sao_Paulo')
agora = datetime.now(brasilia_tz)

minutos_atuais = agora.minute
minutos_proximo_bloco = ((minutos_atuais // WAIT) + 1) * WAIT

if minutos_proximo_bloco == 60:
    proximo_bloco = (agora.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
else:
    proximo_bloco = agora.replace(minute=minutos_proximo_bloco, second=0, microsecond=0)

espera_segundos = (proximo_bloco - agora).total_seconds()

print(f"Aguardando {espera_segundos:.1f} segundos para começar em {proximo_bloco.strftime('%H:%M:%S')}")
time.sleep(espera_segundos)

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
                else:
                    print(f'Não conseguiu obter views para {video_id}')

        # 👉 Espera até o próximo múltiplo de 5 minutos
        agora = datetime.now(brasilia_tz)
        minutos_atuais = agora.minute
        minutos_proximo_bloco = ((minutos_atuais // 5) + 1) * 5
        if minutos_proximo_bloco == 60:
            proximo_bloco = (agora.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
        else:
            proximo_bloco = agora.replace(minute=minutos_proximo_bloco, second=0, microsecond=0)

        espera_segundos = (proximo_bloco - agora).total_seconds()
        print(f"Próxima coleta às {proximo_bloco.strftime('%H:%M:%S')} (em {espera_segundos:.1f} segundos)\n")
        time.sleep(espera_segundos)

except KeyboardInterrupt:
    print("Parado pelo usuário.")
