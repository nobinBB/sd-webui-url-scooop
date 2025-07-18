import os
import re
import requests
import gradio as gr
from modules import script_callbacks, shared
import time
from datetime import datetime

# --- ユーティリティ関数 ---
def get_filename_from_cd(cd_header):
    if not cd_header:
        return None
    m = re.search(r"filename\*=.*''([^;\r\n]+)", cd_header)
    if m:
        return requests.utils.unquote(m.group(1))
    m = re.search(r'filename="?([^";]+)"?', cd_header)
    return m.group(1) if m else None

def convert_civitai_url(url):
    """CivitAIのモデルページURLをダウンロードAPIのURLに変換"""
    # CivitAI モデルページのURLパターンをチェック
    # 例: https://civitai.com/models/1075693?modelVersionId=1207569
    # 例: https://civitai.com/models/1075693/model-name?modelVersionId=1207569
    
    civitai_pattern = r'https://civitai\.com/models/\d+.*?modelVersionId=(\d+)'
    match = re.search(civitai_pattern, url)
    
    if match:
        model_version_id = match.group(1)
        # ダウンロードAPIのURLに変換
        download_url = f"https://civitai.com/api/download/models/{model_version_id}"
        return download_url
    
    # CivitAIのURLでない場合、または既にAPIのURLの場合はそのまま返す
    return url

def read_urls(txt_file, url_text):
    """ファイルまたはテキストからURLリストを取得し、CivitAIのURLを変換"""
    urls = []
    if txt_file:
        # Gradio の UploadedFile は .name にローカルパスを持つ
        path = getattr(txt_file, 'name', None)
        if path and os.path.isfile(path):
            with open(path, encoding='utf-8') as f:
                urls += [ln.strip() for ln in f if ln.strip()]
    if url_text:
        urls += [ln.strip() for ln in url_text.splitlines() if ln.strip()]
    
    # CivitAIのURLを変換
    converted_urls = []
    for url in urls:
        converted_url = convert_civitai_url(url)
        converted_urls.append(converted_url)
    
    return converted_urls

def get_civitai_api_key():
    """設定 or 環境変数から API キーを取得"""
    return (shared.opts.data.get('ch_civiai_api_key')
            or os.getenv('CIVITAI_API_KEY')
            or os.getenv('STABLEDIFFUSION_CIVITAI_API_KEY')
           )

def format_file_size(bytes_size):
    """ファイルサイズを読みやすい形式に変換"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.1f}{unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.1f}TB"

def format_duration(seconds):
    """経過時間を読みやすい形式に変換"""
    if seconds < 60:
        return f"{seconds:.1f}秒"
    elif seconds < 3600:
        return f"{seconds/60:.1f}分"
    else:
        return f"{seconds/3600:.1f}時間"

# --- ダウンロード処理 ---
def batch_download(txt_file, url_text, dest_dir, skip_existing=True, retry_count=3, delay_between_requests=1, progress=gr.Progress()):
    start_time = time.time()
    original_urls = []
    
    # 元のURLリストを取得（変換前）
    if txt_file:
        path = getattr(txt_file, 'name', None)
        if path and os.path.isfile(path):
            with open(path, encoding='utf-8') as f:
                original_urls += [ln.strip() for ln in f if ln.strip()]
    if url_text:
        original_urls += [ln.strip() for ln in url_text.splitlines() if ln.strip()]
    
    # URLを変換
    urls = read_urls(txt_file, url_text)
    
    if not urls:
        return 'URLが指定されていません。ファイルかテキストで入力してください。'
    if not dest_dir:
        return '保存先フォルダを指定してください。'
    
    os.makedirs(dest_dir, exist_ok=True)

    api_key = get_civitai_api_key()
    headers = {'Authorization': f'Bearer {api_key}'} if api_key else {}
    
    # URL変換情報をログに出力
    url_conversions = []
    for i, (original, converted) in enumerate(zip(original_urls, urls)):
        if original != converted:
            url_conversions.append(f'[{i+1:03d}] {original} → {converted}')
    
    logs = [
        f'=== ダウンロード開始 ===',
        f'開始時刻: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        f'合計URL数: {len(urls)}',
        f'保存先: {dest_dir}',
        f'API キー: {"設定済み" if api_key else "未設定（認証エラーの可能性あり）"}',
        f'既存ファイル: {"スキップ" if skip_existing else "上書き"}',
        f'リトライ回数: {retry_count}回',
        f'リクエスト間隔: {delay_between_requests}秒',
        ''
    ]
    
    # URL変換情報があれば表示
    if url_conversions:
        logs.extend(['=== CivitAI URL変換 ==='])
        logs.extend(url_conversions)
        logs.append('')

    success_count = 0
    skip_count = 0
    error_count = 0
    total_downloaded_size = 0
    error_details = []  # エラー詳細を別途保存

    for idx, url in enumerate(urls, 1):
        progress((idx - 1) / len(urls), f"処理中 {idx}/{len(urls)} (成功:{success_count}, スキップ:{skip_count}, エラー:{error_count})")
        
        # リトライロジック
        success = False
        current_error_log = []  # 現在のURLのエラーログ
        
        for attempt in range(retry_count + 1):
            if attempt > 0:
                wait_time = delay_between_requests * (2 ** (attempt - 1))  # 指数バックオフ
                current_error_log.append(f'  -> リトライ {attempt}/{retry_count} ({wait_time}秒待機後)')
                time.sleep(wait_time)
            
            try:
                # ファイル名の事前取得（HEADリクエストで効率化）
                fname = os.path.basename(url.split('?')[0]) or f'file_{idx}'
                out_path = os.path.join(dest_dir, fname)
                
                # 既存ファイルの確認（詳細チェック）
                if skip_existing and os.path.exists(out_path):
                    existing_size = os.path.getsize(out_path)
                    # ファイルサイズが0の場合は破損とみなして再ダウンロード
                    if existing_size > 0:
                        skip_count += 1
                        success = True
                        break
                
                # より正確なファイル名取得のためのHEADリクエスト
                try:
                    head_resp = requests.head(url, headers=headers, timeout=15, allow_redirects=True)
                    if head_resp.status_code == 200:
                        cd = head_resp.headers.get('content-disposition', '')
                        if cd:
                            actual_fname = get_filename_from_cd(cd)
                            if actual_fname:
                                fname = actual_fname
                                out_path = os.path.join(dest_dir, fname)
                                
                                # HEADリクエスト後の再チェック
                                if skip_existing and os.path.exists(out_path):
                                    existing_size = os.path.getsize(out_path)
                                    if existing_size > 0:
                                        skip_count += 1
                                        success = True
                                        break
                except Exception as e:
                    # CivitAI APIの場合、HEADリクエストが失敗することがあるので無視
                    pass
                
                # リクエスト開始（CivitAI APIに対応）
                session = requests.Session()
                session.headers.update(headers)
                
                # CivitAIのAPIの場合、User-Agentを追加
                if 'civitai.com' in url:
                    session.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                
                resp = session.get(url, stream=True, timeout=(15, 300), allow_redirects=True)  # CivitAI用により長いタイムアウト
                resp.raise_for_status()
                
                # ファイル名の最終決定
                cd = resp.headers.get('content-disposition', '')
                final_fname = get_filename_from_cd(cd) or fname
                final_out_path = os.path.join(dest_dir, final_fname)
                
                # 最終ファイル名での重複チェック
                if skip_existing and final_out_path != out_path and os.path.exists(final_out_path):
                    existing_size = os.path.getsize(final_out_path)
                    if existing_size > 0:
                        skip_count += 1
                        resp.close()
                        success = True
                        break
                
                out_path = final_out_path
                fname = final_fname
                
                # Content-Lengthからファイルサイズを取得
                content_length = resp.headers.get('content-length')
                expected_size = int(content_length) if content_length else None
                
                # ディスク容量チェック
                if expected_size:
                    try:
                        disk_free = os.statvfs(dest_dir).f_bavail * os.statvfs(dest_dir).f_frsize
                        if expected_size > disk_free:
                            current_error_log.append(f'  -> [容量不足] 必要:{format_file_size(expected_size)}, 空き:{format_file_size(disk_free)}')
                            error_count += 1
                            break
                    except (OSError, AttributeError):
                        # Windowsの場合、os.statvfsが使えないので容量チェックをスキップ
                        pass
                
                # ファイルダウンロード
                temp_path = out_path + '.tmp'
                
                with open(temp_path, 'wb') as f:
                    for chunk in resp.iter_content(8192):
                        if chunk:
                            f.write(chunk)
                
                # ダウンロード完了、一時ファイルを正式ファイルに移動
                os.rename(temp_path, out_path)
                
                actual_size = os.path.getsize(out_path)
                
                # サイズ不一致の警告
                if expected_size and actual_size != expected_size:
                    current_error_log.append(f'  -> 警告: サイズ不一致 (期待:{format_file_size(expected_size)}, 実際:{format_file_size(actual_size)})')
                
                success_count += 1
                total_downloaded_size += actual_size
                success = True
                break  # リトライループを抜ける
                
            except requests.exceptions.Timeout:
                current_error_log.append(f'  -> [タイムアウト] 試行{attempt + 1}: レスポンスが得られませんでした')
                if attempt == retry_count:
                    error_count += 1
            except requests.exceptions.ConnectionError as e:
                current_error_log.append(f'  -> [接続エラー] 試行{attempt + 1}: {str(e)}')
                if attempt == retry_count:
                    error_count += 1
            except requests.exceptions.HTTPError as e:
                current_error_log.append(f'  -> [HTTPエラー] 試行{attempt + 1}: {resp.status_code}: {str(e)}')
                if attempt == retry_count:
                    error_count += 1
            except requests.exceptions.RequestException as e:
                current_error_log.append(f'  -> [リクエストエラー] 試行{attempt + 1}: {str(e)}')
                if attempt == retry_count:
                    error_count += 1
            except FileNotFoundError:
                current_error_log.append(f'  -> [ファイルエラー] 保存先フォルダにアクセスできません: {dest_dir}')
                error_count += 1
                break  # ファイルエラーはリトライしない
            except PermissionError:
                current_error_log.append(f'  -> [権限エラー] ファイルの書き込み権限がありません: {out_path}')
                error_count += 1
                break  # 権限エラーはリトライしない
            except OSError as e:
                if "No space left on device" in str(e):
                    current_error_log.append(f'  -> [容量不足] ディスク容量が不足しています')
                    error_count += 1
                    break  # 容量不足は致命的エラー
                else:
                    current_error_log.append(f'  -> [OSエラー] 試行{attempt + 1}: {str(e)}')
                    if attempt == retry_count:
                        error_count += 1
            except Exception as e:
                current_error_log.append(f'  -> [予期しないエラー] 試行{attempt + 1}: {type(e).__name__}: {str(e)}')
                if attempt == retry_count:
                    error_count += 1
            finally:
                # 一時ファイルのクリーンアップ
                temp_path = out_path + '.tmp' if 'out_path' in locals() else None
                if temp_path and os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except:
                        pass
        
        # エラーが発生した場合のみログに追加
        if not success:
            if not current_error_log:  # ログが空の場合
                current_error_log = [f'[{idx:03d}/{len(urls):03d}] {url}', f'  -> [最終失敗] {retry_count}回のリトライ後も失敗']
            else:
                current_error_log.insert(0, f'[{idx:03d}/{len(urls):03d}] {url}')
                current_error_log.append(f'  -> [最終失敗] {retry_count}回のリトライ後も失敗')
            
            error_details.extend(current_error_log)
            error_details.append('')  # 空行で区切り
        
        # リクエスト間の待機
        if idx < len(urls) and delay_between_requests > 0:
            time.sleep(delay_between_requests)
    
    # 最終結果のサマリー
    total_time = time.time() - start_time
    
    # エラーがあった場合はエラー詳細を先に表示
    if error_details:
        logs.extend(['=== エラー詳細 ==='])
        logs.extend(error_details)
    
    logs.extend([
        '=== ダウンロード結果 ===',
        f'終了時刻: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        f'処理時間: {format_duration(total_time)}',
        f'成功: {success_count}件',
        f'スキップ: {skip_count}件',
        f'エラー: {error_count}件',
        f'合計: {len(urls)}件',
        f'ダウンロード量: {format_file_size(total_downloaded_size)}',
        ''
    ])
    
    if error_count > 0:
        logs.append('※ エラーが発生したURLがあります。上記エラー詳細で原因を確認してください。')
    
    progress(1.0, "完了")
    return '\n'.join(logs)

# --- Gradio UI ---
def ui():
    with gr.Blocks(analytics_enabled=False) as demo:
        gr.Markdown('## URL Scooop: 一括ダウンロード')
        gr.Markdown('### CivitAI対応: モデルページのURLを自動的にダウンロードAPIのURLに変換します')
        with gr.Row():
            txt_file = gr.File(label='URLリスト(.txt)', file_types=['.txt'])
            url_text = gr.Textbox(
                label='URLリスト(改行区切り) - CivitAIのモデルページURLを貼ってください', 
                lines=4,
                placeholder='例: https://civitai.com/models/1075693?modelVersionId=1207569'
            )
        dest_dir = gr.Textbox(label='保存先フォルダ', placeholder='/path/to/output')
        with gr.Row():
            skip_existing = gr.Checkbox(label='既存ファイルをスキップ', value=True)
            download_mode = gr.Radio(
                choices=[
                    "標準 (リトライ3回, 間隔1秒) - 一般的な用途",
                    "安定重視 (リトライ5回, 間隔2秒) - 不安定な環境",
                    "高速 (リトライ1回, 間隔0.5秒) - 安定した環境",
                    "カスタム"
                ],
                value="標準 (リトライ3回, 間隔1秒) - 一般的な用途",
                label="ダウンロードモード"
            )
        
        # カスタム設定（デフォルトでは非表示）
        with gr.Row(visible=False) as custom_settings:
            retry_count = gr.Slider(minimum=0, maximum=5, value=3, step=1, label='リトライ回数')
            delay_seconds = gr.Slider(minimum=0, maximum=10, value=1, step=0.5, label='リクエスト間隔(秒)')
        
        def toggle_custom(mode):
            return gr.update(visible=mode == "カスタム")
        
        download_mode.change(toggle_custom, download_mode, custom_settings)
        
        btn = gr.Button('ダウンロード開始')
        log_output = gr.Textbox(label='ログ', lines=20)
        
        def start_download(txt_file, url_text, dest_dir, skip_existing, mode, retry_count, delay_seconds, progress=gr.Progress()):
            # モードから設定を決定
            if mode.startswith("標準"):
                retry_count, delay_seconds = 3, 1
            elif mode.startswith("安定重視"):
                retry_count, delay_seconds = 5, 2
            elif mode.startswith("高速"):
                retry_count, delay_seconds = 1, 0.5
            # カスタムの場合はそのまま使用
            
            return batch_download(txt_file, url_text, dest_dir, skip_existing, retry_count, delay_seconds, progress)
        
        btn.click(start_download, [txt_file, url_text, dest_dir, skip_existing, download_mode, retry_count, delay_seconds], log_output)
    return demo

# WebUI にタブ追加
script_callbacks.on_ui_tabs(lambda: [(ui(), 'URL Scooop', 'url_scooop')])