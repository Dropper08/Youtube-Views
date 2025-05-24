import requests
import time
from datetime import datetime, timedelta
import pytz
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, DateTime, MetaData

# 👉 Configurações da API e vídeo
API_KEY = 'AIzaSyACx1i4XGXJjRvQJukTTvZCvD6FNexhgmg'
VIDEO_ID = '-4GmbBoYQjE'

# 👉 Configurações do banco PostgreSQL (Railway)
DB_URL = 'postgresql://postgres:DqVPbefCrJJneICVKwPTOUozzSmUjusn@postgres.railway.internal:5432/railway'

# 👉 Cria engine do SQLAlchemy
engine = create_engine(DB_URL)
metadata = MetaData()

# 👉 Define a tabela (se não existir, cria)
views_table = Table(
    'youtube_views', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('horario', DateTime(timezone=True)),
    Column('views', Integer)
)

metadata.create_all(engine)

# 👉 Função para buscar dados do vídeo
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


# 👉 Alinha para o próximo múltiplo de 5 minutos
brasilia_tz = pytz.timezone('America/Sao_Paulo')
agora = datetime.now(brasilia_tz)

minutos_atuais = agora.minute
minutos_proximo_bloco = ((minutos_atuais // 5) + 1) * 5

if minutos_proximo_bloco == 60:
    proximo_bloco = (agora.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
else:
    proximo_bloco = agora.replace(minute=minutos_proximo_bloco, second=0, microsecond=0)

espera_segundos = (proximo_bloco - agora).total_seconds()

print(f"Aguardando {espera_segundos:.1f} segundos para começar em {proximo_bloco.strftime('%H:%M:%S')}")
time.sleep(espera_segundos)

# 👉 Inicializa variáveis
previous_views = None
previous_time = None
previous_delta = None

try:
    while True:
        agora_brasilia = datetime.now(brasilia_tz)
        views = get_video_stats(VIDEO_ID, API_KEY)
        hora = agora_brasilia

        if views is not None:
            print(f'Views at {hora.strftime("%Y-%m-%d %H:%M:%S")} : {views}')

            # 👉 Salva no banco de dados
            with engine.connect() as conn:
                conn.execute(views_table.insert().values(horario=hora, views=views))

            # 👉 Lê dados do banco para análise
            df = pd.read_sql_table('youtube_views', con=engine)
            df['horario'] = pd.to_datetime(df['horario']).dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo')
            df = df.sort_values('horario')

            if previous_views is not None:
                delta_views = views - previous_views
                minutes_passed = (agora_brasilia - previous_time).total_seconds() / 60
                if minutes_passed > 0:
                    views_per_hour = (delta_views / minutes_passed) * 60
                else:
                    views_per_hour = 0
                if previous_delta is not None and previous_delta != 0:
                    print(f'Ganhos nos últimos {minutes_passed:.0f} minutos: \n{delta_views} views : {((delta_views/previous_delta)-1)*100:.2f}% delta')
                else:
                    print(f'Ganhos nos últimos {minutes_passed:.0f} minutos: {delta_views} views')

                previous_delta = delta_views
                print(f'Média estimada por hora: {views_per_hour:.2f} views/hora')
            else:
                print('Primeira medição — aguardando próxima para calcular diferenças.')

            previous_views = views
            previous_time = agora_brasilia

            # 📊 Cálculo últimos 15 e 30 minutos
            def get_delta_views(df, minutos):
                limite = agora_brasilia - timedelta(minutes=minutos)
                df_filtrado = df[df['horario'] >= limite]
                if len(df_filtrado) >= 2:
                    return df_filtrado['views'].iloc[-1] - df_filtrado['views'].iloc[0]
                return 0

            v15 = get_delta_views(df, 15)
            v30 = get_delta_views(df, 30)

            print(f'↳ Total nos últimos 15 minutos: {v15} views')
            print(f'↳ Total nos últimos 30 minutos: {v30} views')

        else:
            print("Não foi possível obter as views.")

        # 👉 Espera até o próximo múltiplo de 5 minutos
        agora = datetime.now(brasilia_tz)
        minutos_atuais = agora.minute
        minutos_proximo_bloco = ((minutos_atuais // 5) + 1) * 5
        proximo_bloco = agora.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=minutos_proximo_bloco)
        espera_segundos = (proximo_bloco - agora).total_seconds()
        print(f"Próxima coleta marcada para {proximo_bloco.strftime('%H:%M:%S')} (em {espera_segundos:.1f} segundos)\n")
        time.sleep(espera_segundos)

except KeyboardInterrupt:
    print("Parado pelo usuário.")
