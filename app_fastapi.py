import os
import re
import time
import logging
import threading
import pandas as pd
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest
from jinja2 import Environment, FileSystemLoader
from threading import Lock
import asyncio

VERB_CSV_PATH = os.environ.get("VERB_CSV_PATH", "/app/verb.csv")
ADJECTIVE_CSV_PATH = os.environ.get("ADJECTIVE_CSV_PATH", "/app/adjective.csv")

app = FastAPI(title="Japanese Verb and Adjective Conjugation")

# リクエスト処理時間を記録するミドルウェア（502エラー完全防止のため、タイムアウトを完全に無効化）
class TimingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: StarletteRequest, call_next):
        start_time = time.time()
        request_id = f"{request.client.host if request.client else 'unknown'}:{request.client.port if request.client else 'unknown'}-{int(start_time * 1000)}"
        is_head = request.method == "HEAD"
        
        # HEADリクエストの場合は、即座にレスポンスを返す（502エラー完全防止）
        if is_head:
            logger.info(f"📥 Middleware: HEAD Request {request_id} started: {request.url.path}")
            try:
                # 即座にレスポンスを返す（処理は不要）
                response = HTMLResponse(content="", status_code=200)
                response.headers["X-Process-Time"] = "0.000"
                response.headers["X-Request-ID"] = request_id
                response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
                response.headers["Pragma"] = "no-cache"
                response.headers["Expires"] = "0"
                response.headers["CF-Cache-Status"] = "DYNAMIC"
                response.headers["Connection"] = "keep-alive"
                process_time = time.time() - start_time
                logger.info(f"⏱️ Middleware: HEAD Request {request_id} completed in {process_time:.3f}s, status: 200")
                return response
            except Exception as e:
                logger.error(f"❌ Middleware: HEAD Request {request_id} failed: {e}", exc_info=True)
                # エラーでも200を返す（502エラーを避けるため）
                response = HTMLResponse(content="", status_code=200)
                response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
                response.headers["CF-Cache-Status"] = "DYNAMIC"
                response.headers["Connection"] = "keep-alive"
                return response
        
        logger.info(f"📥 Middleware: Request {request_id} started: {request.method} {request.url.path}")
        try:
            # リクエストを確実に処理（タイムアウトなし）
            # エラーが発生しても、必ずレスポンスを返す（502エラーを避けるため）
            try:
                response = await call_next(request)
            except Exception as inner_error:
                # 内部エラーが発生した場合でも、適切なレスポンスを返す
                logger.error(f"❌ Inner error in request {request_id}: {inner_error}", exc_info=True)
                try:
                    response = HTMLResponse(content=render_template("error.html", error_message=f"リクエスト処理中にエラーが発生しました: {str(inner_error)}"), status_code=500)
                except:
                    response = HTMLResponse(content=f"<h1>エラーが発生しました</h1><p>{str(inner_error)}</p><p><a href='/'>トップページに戻る</a></p>", status_code=500)
            
            process_time = time.time() - start_time
            logger.info(f"⏱️ Middleware: Request {request_id} completed in {process_time:.3f}s, status: {response.status_code}")
            if process_time > 1.0:
                logger.warning(f"⚠️ Slow request {request_id}: {request.method} {request.url.path} took {process_time:.3f}s")
            
            # レスポンスヘッダーに処理時間を追加（デバッグ用）
            response.headers["X-Process-Time"] = f"{process_time:.3f}"
            response.headers["X-Request-ID"] = request_id
            # Cloudflare用のヘッダーを追加（タイムアウト防止）
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
            # Cloudflareにキャッシュしないように指示
            response.headers["CF-Cache-Status"] = "DYNAMIC"
            # 接続を保持
            response.headers["Connection"] = "keep-alive"
            # Content-LengthとTransfer-Encodingの競合を避けるため、Content-Lengthを削除
            # Uvicornが自動的にTransfer-Encoding: chunkedを設定する場合があるため
            if "Content-Length" in response.headers:
                del response.headers["Content-Length"]
            return response
        except Exception as e:
            process_time = time.time() - start_time
            logger.error(f"❌ Middleware: Request {request_id} failed after {process_time:.3f}s: {e}", exc_info=True)
            # エラーでも502エラーではなく、適切なエラーページを返す
            try:
                error_response = HTMLResponse(content=render_template("error.html", error_message=f"リクエスト処理中にエラーが発生しました: {str(e)}"), status_code=500)
                error_response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
                error_response.headers["CF-Cache-Status"] = "DYNAMIC"
                error_response.headers["Connection"] = "keep-alive"
                # Content-LengthとTransfer-Encodingの競合を避けるため、Content-Lengthを削除
                if "Content-Length" in error_response.headers:
                    del error_response.headers["Content-Length"]
                return error_response
            except:
                error_response = HTMLResponse(content=f"<h1>エラーが発生しました</h1><p>{str(e)}</p><p><a href='/'>トップページに戻る</a></p>", status_code=500)
                error_response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
                error_response.headers["CF-Cache-Status"] = "DYNAMIC"
                error_response.headers["Connection"] = "keep-alive"
                # Content-LengthとTransfer-Encodingの競合を避けるため、Content-Lengthを削除
                if "Content-Length" in error_response.headers:
                    del error_response.headers["Content-Length"]
                return error_response

app.add_middleware(TimingMiddleware)

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True
)
logger = logging.getLogger(__name__)

# グローバルエラーハンドラー
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """すべての例外を処理し、502エラーを避ける"""
    import sys
    error_msg = f"❌ Unhandled exception: {exc}"
    print(error_msg, flush=True)
    sys.stderr.write(error_msg + "\n")
    sys.stderr.flush()
    logger.error(error_msg, exc_info=True)
    try:
        return HTMLResponse(content=render_template("error.html", error_message=f"エラーが発生しました: {str(exc)}"), status_code=500)
    except Exception as render_error:
        return HTMLResponse(content=f"<h1>エラーが発生しました</h1><p>{str(exc)}</p><p><a href='/'>トップページに戻る</a></p>", status_code=500)

# テンプレート設定
jinja_env = Environment(loader=FileSystemLoader("templates"))

def render_template(template_name: str, **kwargs):
    """テンプレートをレンダリング"""
    template = jinja_env.get_template(template_name)
    return template.render(**kwargs)

# 起動時に必ずログを出力
print("=" * 60, flush=True)
print("🚀 Application starting (FastAPI)...", flush=True)
print(f"📁 VERB_CSV_PATH will be: {VERB_CSV_PATH}", flush=True)
print(f"📁 ADJECTIVE_CSV_PATH will be: {ADJECTIVE_CSV_PATH}", flush=True)
print("=" * 60, flush=True)

# キャッシュ用のロック
_cache_lock = Lock()
_cache_data = None
_cache_timestamp = 0
_cache_loading = False
CACHE_TTL = 600
CACHE_REFRESH_THRESHOLD = 540

def load_csv_data(csv_path):
    """CSVデータをローカルファイルから読み込む（高速）"""
    try:
        msg = f"🚀 Loading CSV from local file: {csv_path}"
        print(msg, flush=True)
        logger.info(msg)
        
        if not os.path.exists(csv_path):
            error_msg = f"CSV file not found: {csv_path}"
            print(f"❌ {error_msg}", flush=True)
            logger.error(f"❌ {error_msg}")
            raise FileNotFoundError(error_msg)
        
        file_size = os.path.getsize(csv_path)
        size_msg = f"📊 CSV file size: {file_size:,} bytes ({file_size/1024:.2f} KB)"
        print(size_msg, flush=True)
        logger.info(size_msg)
        
        start_time = time.time()
        
        encodings = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932', 'latin-1']
        df = None
        
        for encoding in encodings:
            try:
                encoding_msg = f"Trying encoding: {encoding}"
                print(encoding_msg, flush=True)
                logger.info(encoding_msg)
                df = pd.read_csv(csv_path, encoding=encoding)
                success_msg = f"✅ CSV loaded successfully with encoding: {encoding}"
                print(success_msg, flush=True)
                logger.info(success_msg)
                break
            except UnicodeDecodeError:
                logger.debug(f"UnicodeDecodeError with encoding {encoding}, trying next...")
                continue
            except Exception as e:
                warning_msg = f"Failed to load with encoding {encoding}: {e}"
                print(warning_msg, flush=True)
                logger.warning(warning_msg)
                continue
        
        if df is None:
            error_msg = f"Failed to load CSV with any encoding. Tried: {encodings}"
            print(f"❌ {error_msg}", flush=True)
            logger.error(f"❌ {error_msg}")
            raise ValueError(error_msg)
        
        elapsed = time.time() - start_time
        shape_msg = f"✅ CSV loaded in {elapsed:.3f}s, shape: {df.shape} (rows: {df.shape[0]}, cols: {df.shape[1]})"
        print(shape_msg, flush=True)
        logger.info(shape_msg)
        return df
    except FileNotFoundError as e:
        error_msg = f"❌ CSV file not found: {csv_path}"
        print(error_msg, flush=True)
        logger.error(error_msg)
        logger.error(f"Current working directory: {os.getcwd()}")
        logger.error(f"Files in /app: {os.listdir('/app') if os.path.exists('/app') else 'N/A'}")
        raise
    except Exception as e:
        error_msg = f"❌ Error loading CSV: {e}"
        print(error_msg, flush=True)
        logger.error(error_msg, exc_info=True)
        raise

def load_data():
    """データを読み込む（verb.csvとadjective.csvの両方）"""
    all_chunks = []
    
    # verb.csvを読み込む
    if os.path.exists(VERB_CSV_PATH):
        check_msg = f"🔍 Checking for verb CSV file at: {VERB_CSV_PATH}"
        print(check_msg, flush=True)
        logger.info(check_msg)
        found_msg = f"✅ Verb CSV file found! Using local CSV file - {VERB_CSV_PATH}"
        print(found_msg, flush=True)
        logger.info(found_msg)
        verb_df = load_csv_data(VERB_CSV_PATH)
        verb_chunks = split_data_into_chunks(verb_df, source='verb')
        all_chunks.extend(verb_chunks)
    else:
        warning_msg = f"⚠️ Verb CSV file NOT found at {VERB_CSV_PATH}. Skipping."
        print(warning_msg, flush=True)
        logger.warning(warning_msg)
    
    # adjective.csvを読み込む
    if os.path.exists(ADJECTIVE_CSV_PATH):
        check_msg = f"🔍 Checking for adjective CSV file at: {ADJECTIVE_CSV_PATH}"
        print(check_msg, flush=True)
        logger.info(check_msg)
        found_msg = f"✅ Adjective CSV file found! Using local CSV file - {ADJECTIVE_CSV_PATH}"
        print(found_msg, flush=True)
        logger.info(found_msg)
        adjective_df = load_csv_data(ADJECTIVE_CSV_PATH)
        adjective_chunks = split_data_into_chunks(adjective_df, source='adjective')
        all_chunks.extend(adjective_chunks)
    else:
        warning_msg = f"⚠️ Adjective CSV file NOT found at {ADJECTIVE_CSV_PATH}. Skipping."
        print(warning_msg, flush=True)
        logger.warning(warning_msg)
    
    if len(all_chunks) == 0:
        error_msg = f"❌ No CSV files found. Checked: {VERB_CSV_PATH}, {ADJECTIVE_CSV_PATH}"
        print(error_msg, flush=True)
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    return all_chunks

def split_data_into_chunks(df, source='verb'):
    """データを4行ごとの塊に分割し、各塊の情報を返す
    
    Args:
        df: データフレーム
        source: データソース（'verb'または'adjective'）
    """
    chunks = []
    start_idx = 0
    while start_idx < len(df):
        end_idx = start_idx + 4
        if end_idx > len(df):
            end_idx = len(df)
        
        chunk_df = df.iloc[start_idx:end_idx].copy()
        
        if len(chunk_df) > 0:
            title = str(chunk_df.iloc[0, 0]).strip()
            # B列（2列目）の値を取得（存在する場合）
            subtitle = ""
            if len(chunk_df.columns) > 1:
                subtitle_value = chunk_df.iloc[0, 1]
                if pd.notna(subtitle_value):
                    subtitle = str(subtitle_value).strip()
            # ソースを考慮したスラッグ生成（重複を避けるため）
            base_slug = re.sub(r'[^\w\s-]', '', title.lower())
            base_slug = re.sub(r'[-\s]+', '-', base_slug)
            base_slug = base_slug.strip('-')
            # ソースをスラッグに含める（重複を避けるため）
            slug = f"{base_slug}-{source}" if source else base_slug
            
            chunks.append({
                'title': title,
                'subtitle': subtitle,  # B列の値
                'slug': slug,
                'source': source,  # 'verb'または'adjective'
                'data': chunk_df,
                'columns': df.columns.tolist()
            })
        
        start_idx = end_idx
    
    return chunks

def get_all_chunks():
    """すべてのチャンクを取得（キャッシュ付き、Stale-While-Revalidate）"""
    global _cache_data, _cache_timestamp, _cache_loading
    
    current_time = time.time()
    
    # キャッシュが有効な場合、すぐに返す
    with _cache_lock:
        if _cache_data is not None:
            cache_age = current_time - _cache_timestamp
            if cache_age < CACHE_TTL:
                return _cache_data
            
            # キャッシュが古いが、まだ有効な場合（Stale-While-Revalidate）
            if cache_age < CACHE_TTL * 2:
                # バックグラウンドで更新を開始（既に更新中でない場合）
                if not _cache_loading:
                    _cache_loading = True
                    # バックグラウンドで更新（ロックを解放してから）
                    threading.Thread(target=_refresh_cache, daemon=True).start()
                # 古いキャッシュを返す
                return _cache_data
    
    # キャッシュがない、または完全に無効な場合、同期的に更新
    return _refresh_cache()

def _refresh_cache():
    """キャッシュを更新"""
    global _cache_data, _cache_timestamp, _cache_loading
    
    try:
        chunks = load_data()  # load_data()は既にチャンクのリストを返す
        
        with _cache_lock:
            _cache_data = chunks
            _cache_timestamp = time.time()
            _cache_loading = False
        
        logger.info(f"✅ Cache refreshed: {len(chunks)} chunks")
        return chunks
    except Exception as e:
        with _cache_lock:
            _cache_loading = False
        logger.error(f"❌ Cache refresh failed: {e}", exc_info=True)
        # エラー時はキャッシュがあればそれを返す
        if _cache_data is not None:
            logger.warning(f"Using stale cache due to error: {e}")
            return _cache_data
        raise

# 初期化フラグ
_initialized = False
_init_lock = Lock()

def ensure_initialized():
    """最初のリクエスト時に初期化を実行"""
    global _initialized
    
    if _initialized:
        return
    
    init_start_time = time.time()
    max_wait_time = 5.0
    
    while not _initialized and (time.time() - init_start_time) < max_wait_time:
        with _init_lock:
            if _initialized:
                return
            if _cache_loading and _cache_data is not None:
                _initialized = True
                return
        time.sleep(0.1)
    
    with _init_lock:
        if _initialized:
            return
        
        try:
            import sys
            sys.stdout.write("=" * 60 + "\n")
            sys.stdout.write("🚀 Starting application initialization...\n")
            sys.stdout.write(f"📁 VERB_CSV_PATH: {VERB_CSV_PATH}\n")
            sys.stdout.write(f"📁 ADJECTIVE_CSV_PATH: {ADJECTIVE_CSV_PATH}\n")
            sys.stdout.write(f"📂 Current working directory: {os.getcwd()}\n")
            sys.stdout.flush()
            
            logger.info("=" * 60)
            logger.info("🚀 Starting application initialization...")
            logger.info(f"📁 VERB_CSV_PATH: {VERB_CSV_PATH}")
            logger.info(f"📁 ADJECTIVE_CSV_PATH: {ADJECTIVE_CSV_PATH}")
            logger.info(f"📂 Current working directory: {os.getcwd()}")
            
            found_files = []
            if os.path.exists(VERB_CSV_PATH):
                file_size = os.path.getsize(VERB_CSV_PATH)
                msg = f"✅ Verb CSV file found! Size: {file_size:,} bytes ({file_size/1024:.2f} KB)"
                sys.stdout.write(msg + "\n")
                sys.stdout.flush()
                logger.info(msg)
                found_files.append(VERB_CSV_PATH)
            else:
                warning_msg = f"⚠️ Verb CSV file NOT found at {VERB_CSV_PATH}"
                sys.stdout.write(warning_msg + "\n")
                sys.stdout.flush()
                logger.warning(warning_msg)
            
            if os.path.exists(ADJECTIVE_CSV_PATH):
                file_size = os.path.getsize(ADJECTIVE_CSV_PATH)
                msg = f"✅ Adjective CSV file found! Size: {file_size:,} bytes ({file_size/1024:.2f} KB)"
                sys.stdout.write(msg + "\n")
                sys.stdout.flush()
                logger.info(msg)
                found_files.append(ADJECTIVE_CSV_PATH)
            else:
                warning_msg = f"⚠️ Adjective CSV file NOT found at {ADJECTIVE_CSV_PATH}"
                sys.stdout.write(warning_msg + "\n")
                sys.stdout.flush()
                logger.warning(warning_msg)
            
            if len(found_files) == 0:
                error_msg = f"❌ No CSV files found. Checked: {VERB_CSV_PATH}, {ADJECTIVE_CSV_PATH}"
                sys.stderr.write(error_msg + "\n")
                sys.stderr.flush()
                logger.error(error_msg)
                if os.path.exists('/app'):
                    sys.stdout.write("📋 Files in /app directory:\n")
                    sys.stdout.flush()
                    logger.info("📋 Files in /app directory:")
                    for f in os.listdir('/app'):
                        full_path = os.path.join('/app', f)
                        if os.path.isfile(full_path):
                            size = os.path.getsize(full_path)
                            file_info = f"   - {f} ({size:,} bytes)"
                            sys.stdout.write(file_info + "\n")
                            sys.stdout.flush()
                            logger.info(file_info)
                        else:
                            dir_info = f"   - {f}/ (directory)"
                            sys.stdout.write(dir_info + "\n")
                            sys.stdout.flush()
                            logger.info(dir_info)
                raise FileNotFoundError(error_msg)
            
            sys.stdout.write("📦 Preloading cache on startup...\n")
            sys.stdout.flush()
            logger.info("📦 Preloading cache on startup...")
            get_all_chunks()
            sys.stdout.write("✅ Cache preloaded successfully\n")
            sys.stdout.write("=" * 60 + "\n")
            sys.stdout.flush()
            logger.info("✅ Cache preloaded successfully")
            logger.info("=" * 60)
            
            _initialized = True
        except Exception as e:
            error_msg = f"❌ Failed to preload cache: {e}"
            import sys
            sys.stderr.write(error_msg + "\n")
            sys.stderr.flush()
            logger.error(error_msg, exc_info=True)
            raise

@app.get("/", response_class=HTMLResponse)
@app.head("/")
async def index(request: Request, q: str = ""):
    """トップページ：検索窓のみ"""
    import sys
    request_start = time.time()
    request_id = f"{request.client.host}:{request.client.port}-{int(request_start * 1000)}"
    is_head = request.method == "HEAD"
    
    # HEADリクエストの場合は、すぐにレスポンスを返す（初期化チェックも不要）
    if is_head:
        response = HTMLResponse(content="", status_code=200)
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["CF-Cache-Status"] = "DYNAMIC"
        return response
    
    logger.info(f"📥 Request {request_id}: {request.method} /?q={q}")
    try:
        ensure_initialized()
        
        query = q.strip()
        results = []
        
        if query:
            msg = f"🔍 Search request received: '{query}'"
            print(msg, flush=True)
            logger.info(msg)
            
            try:
                start_time = time.time()
                chunks = get_all_chunks()
                load_time = time.time() - start_time
                msg = f"📦 Chunks loaded in {load_time:.3f}s (total: {len(chunks)} chunks)"
                print(msg, flush=True)
                logger.info(msg)
                
                query_lower = query.lower()
                search_start = time.time()
                
                chunk_count = 0
                for chunk in chunks:
                    chunk_count += 1
                    try:
                        if query_lower in chunk['title'].lower():
                            results.append(chunk)
                            continue
                        
                        found = False
                        for col in chunk['data'].columns:
                            if found:
                                break
                            col_values = chunk['data'][col].astype(str).str.lower()
                            if col_values.str.contains(query_lower, na=False).any():
                                results.append(chunk)
                                found = True
                                break
                    except Exception as chunk_error:
                        logger.warning(f"Error processing chunk {chunk_count}: {chunk_error}")
                        continue
                
                search_time = time.time() - search_start
                msg = f"✅ Search completed in {search_time:.3f}s, found {len(results)} results (searched {chunk_count} chunks)"
                print(msg, flush=True)
                logger.info(msg)
            except Exception as search_error:
                error_msg = f"❌ Search error: {search_error}"
                print(error_msg, flush=True)
                sys.stderr.write(error_msg + "\n")
                sys.stderr.flush()
                logger.error(error_msg, exc_info=True)
                results = []
        
        total_time = time.time() - request_start
        logger.info(f"✅ Request {request_id} completed in {total_time:.3f}s")
        if total_time > 1.0:
            msg = f"⚠️ Slow request {request_id}: {total_time:.3f}s"
            print(msg, flush=True)
            logger.warning(msg)
        
        response = HTMLResponse(content=render_template("index.html", query=query, results=results))
        logger.info(f"📤 Request {request_id}: Sending response (status: 200)")
        return response
    except Exception as e:
        error_msg = f"❌ Error in index: {e}"
        print(error_msg, flush=True)
        sys.stderr.write(error_msg + "\n")
        sys.stderr.flush()
        logger.error(error_msg, exc_info=True)
        try:
            return HTMLResponse(content=render_template("error.html", error_message=f"エラーが発生しました: {str(e)}"), status_code=500)
        except Exception as render_error:
            return HTMLResponse(content=f"<h1>エラーが発生しました</h1><p>{str(e)}</p>", status_code=500)

@app.get("/health")
@app.head("/health")
async def health(request: Request):
    """ヘルスチェックエンドポイント"""
    # HEADリクエストの場合は、すぐにレスポンスを返す（初期化チェックも不要）
    if request.method == "HEAD":
        response = HTMLResponse(content="", status_code=200)
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["CF-Cache-Status"] = "DYNAMIC"
        return response
    
    try:
        ensure_initialized()
        
        with _cache_lock:
            if _cache_data is not None:
                cache_age = time.time() - _cache_timestamp
                response_text = f"OK (cache age: {int(cache_age)}s)"
            else:
                response_text = "OK (cache not ready)"
        
        return response_text
    except Exception as e:
        error_msg = f"Health check error: {e}"
        print(error_msg, flush=True)
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=503, detail="ERROR")

@app.get("/{slug}", response_class=HTMLResponse)
@app.head("/{slug}")
async def page_detail(request: Request, slug: str):
    """個別ページ：各塊の詳細表示"""
    import sys
    request_start = time.time()
    request_id = f"{request.client.host}:{request.client.port}-{int(request_start * 1000)}"
    is_head = request.method == "HEAD"
    
    # HEADリクエストの場合は、軽量なチェックのみ（初期化チェックは不要）
    if is_head:
        try:
            # キャッシュが存在するかどうかのみチェック
            with _cache_lock:
                if _cache_data is None:
                    # キャッシュがない場合は、すぐに200を返す（存在チェックはスキップ）
                    response = HTMLResponse(content="", status_code=200)
                    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
                    response.headers["CF-Cache-Status"] = "DYNAMIC"
                    return response
            
            # キャッシュがある場合は、軽量な存在チェック
            chunks = get_all_chunks()
            chunk = None
            for c in chunks:
                if c['slug'] == slug:
                    chunk = c
                    break
            if not chunk:
                response = HTMLResponse(content="", status_code=404)
                response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
                response.headers["CF-Cache-Status"] = "DYNAMIC"
                return response
            response = HTMLResponse(content="", status_code=200)
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["CF-Cache-Status"] = "DYNAMIC"
            return response
        except Exception:
            # エラーが発生した場合でも、200を返す（502エラーを避けるため）
            response = HTMLResponse(content="", status_code=200)
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["CF-Cache-Status"] = "DYNAMIC"
            return response
    
    logger.info(f"📥 Request {request_id}: {request.method} /{slug}")
    try:
        msg = f"📄 Page detail request: {slug}"
        print(msg, flush=True)
        logger.info(msg)
        
        ensure_initialized()
        
        start_time = time.time()
        chunks = get_all_chunks()
        load_time = time.time() - start_time
        msg = f"📦 Chunks loaded in {load_time:.3f}s (total: {len(chunks)} chunks)"
        print(msg, flush=True)
        logger.info(msg)
        
        chunk = None
        search_start = time.time()
        for c in chunks:
            if c['slug'] == slug:
                chunk = c
                break
        search_time = time.time() - search_start
        
        if not chunk:
            msg = f"❌ Page not found: {slug} (searched {len(chunks)} chunks in {search_time:.3f}s)"
            print(msg, flush=True)
            logger.warning(msg)
            return HTMLResponse(content=render_template("error.html", error_message=f"ページが見つかりません: {slug}"), status_code=404)
        
        msg = f"✅ Found chunk: {chunk['title']} (search took {search_time:.3f}s)"
        print(msg, flush=True)
        logger.info(msg)
        
        try:
            if len(chunk['data'].columns) > 1:
                display_df = chunk['data'].iloc[:, 1:].copy()
            else:
                display_df = chunk['data'].copy()
            
            display_df = display_df.fillna('')
            
            # 改行コードを削除（セル内の文字は絶対に改行しない）
            for col in display_df.columns:
                # すべての種類の改行コードを削除
                display_df[col] = display_df[col].astype(str).str.replace('\n', ' ', regex=False).str.replace('\r', ' ', regex=False).str.replace('\r\n', ' ', regex=False)
                # 連続するスペースを1つにまとめる
                display_df[col] = display_df[col].str.replace(r'\s+', ' ', regex=True).str.strip()
            
            table_start = time.time()
            table_html = display_df.to_html(
                classes="table",
                index=False,
                border=0,
                escape=False,  # HTMLタグをエスケープしない
            )
            # 生成されたHTMLからも改行コードと<br>タグを削除（動詞・形容詞共通の処理）
            # <td>と</td>の間の改行を削除（セル内のテキストを1行に保つ）
            def clean_cell_content(match):
                """セル内のコンテンツから改行を完全に削除"""
                tag_start = match.group(1)
                content = match.group(2)
                tag_end = match.group(3)
                # すべての改行コード、<br>タグ、連続するスペースを削除
                cleaned = re.sub(r'<br\s*/?>', ' ', content, flags=re.IGNORECASE)
                cleaned = cleaned.replace('\n', ' ').replace('\r', '').replace('\t', ' ')
                cleaned = re.sub(r'\s+', ' ', cleaned).strip()
                return tag_start + cleaned + tag_end
            
            table_html = re.sub(r'(<td[^>]*>)(.*?)(</td>)', clean_cell_content, table_html, flags=re.DOTALL)
            # <th>と</th>の間の改行も削除
            table_html = re.sub(r'(<th[^>]*>)(.*?)(</th>)', clean_cell_content, table_html, flags=re.DOTALL)
            # HTML全体の改行コードを削除（ただし、タグ間の構造は保持）
            table_html = re.sub(r'\n\s*', ' ', table_html)
            table_html = re.sub(r'\r\s*', ' ', table_html)
            table_html = re.sub(r'\s+', ' ', table_html)
            # タグ間の不要なスペースを整理
            table_html = re.sub(r'>\s+<', '><', table_html)
            table_elapsed = time.time() - table_start
            logger.info(f"📊 Table HTML generated in {table_elapsed:.3f}s")
            
            total_elapsed = time.time() - request_start
            logger.info(f"✅ Request {request_id} (page_detail) completed in {total_elapsed:.3f}s")
            if total_elapsed > 1.0:
                logger.warning(f"⚠️ Slow page detail {request_id}: {total_elapsed:.3f}s for slug '{slug}'")
            
            response = HTMLResponse(content=render_template("detail.html", title=chunk['title'], table_html=table_html))
            logger.info(f"📤 Request {request_id}: Sending response (status: 200)")
            return response
        except Exception as table_error:
            error_msg = f"❌ Error generating table: {table_error}"
            print(error_msg, flush=True)
            logger.error(error_msg, exc_info=True)
            return HTMLResponse(content=render_template("error.html", error_message=f"テーブル生成エラー: {str(table_error)}"), status_code=500)
    except Exception as e:
        error_msg = f"❌ Error in page_detail: {e}"
        print(error_msg, flush=True)
        sys.stderr.write(error_msg + "\n")
        sys.stderr.flush()
        logger.error(error_msg, exc_info=True)
        try:
            return HTMLResponse(content=render_template("error.html", error_message=f"エラーが発生しました: {str(e)}"), status_code=500)
        except Exception as render_error:
            return HTMLResponse(content=f"<h1>エラーが発生しました</h1><p>{str(e)}</p>", status_code=500)

# 起動時の初期化
try:
    import sys
    if not _initialized:
        try:
            print("🚀 Attempting to preload application at startup...", flush=True)
            ensure_initialized()
            print("✅ Application preloaded successfully at startup", flush=True)
        except Exception as preload_error:
            print(f"⚠️ Preload failed (will initialize on first request): {preload_error}", flush=True)
            sys.stderr.write(f"⚠️ Preload failed (will initialize on first request): {preload_error}\n")
            sys.stderr.flush()
except Exception as e:
    pass

