FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app_fastapi.py .
COPY templates ./templates

# CSVファイルをコピー（存在確認付き）
COPY verb.csv /app/verb.csv

# ファイルが正しくコピーされたか確認
RUN ls -lh /app/verb.csv || (echo "ERROR: verb.csv not found!" && exit 1)

EXPOSE 8000

# 本番環境用のUvicornを使用（FastAPI）
# workers: ワーカー数（2ワーカーで負荷分散）
# timeout-keep-alive: 接続を保持する時間（秒）- 実質的に無制限
# timeout-graceful-shutdown: グレースフルシャットダウンの猶予時間
# limit-concurrency: 同時接続数の制限を緩和
# backlog: 接続キューサイズを増やす
# log-level: ログレベル
CMD ["uvicorn", "app_fastapi:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4", "--timeout-keep-alive", "86400", "--timeout-graceful-shutdown", "3600", "--limit-concurrency", "4000", "--backlog", "8192", "--log-level", "info", "--access-log", "--no-use-colors"]

