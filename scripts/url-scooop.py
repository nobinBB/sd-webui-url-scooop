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

def read_urls(txt_file, url_text):
    """ファイルまたはテキストからURLリストを取得"""
    urls = []
    if txt_file:
        # Gradio の UploadedFile は .name にローカルパスを持つ
        path = getattr(txt_file, 'name', None)
        if path and os.path.isfile(path):
            with open(path, encoding='utf-8') as f:
                urls += [ln.strip() for ln in f if ln.strip()]
    if url_text:
        urls += [ln.strip() for ln in url_text.splitlines() if ln.strip()]
    return urls

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
def batch_download(txt_file, url_text, dest_dir, skip_existing=True, progress=gr.Progress()):
    start_time = time.time()
    urls = read_urls(txt_file, url_text)
    
    if not urls:
        return 'URLが指定されていません。ファイルかテキストで入力してください。'
    if not dest_dir:
        return '保存先フォルダを指定してください。'
    
    os.makedirs(dest_dir, exist_ok=True)

    api_key = get_civitai_api_key()
    headers = {'Authorization': f'Bearer {api_key}'} if api_key else {}
    
    logs = [
        f'=== ダウンロード開始 ===',
        f'開始時刻: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        f'合計URL数: {len(urls)}',
        f'保存先: {dest_dir}',
        f'API キー: {"設定済み" if api_key else "未設定（認証エラーの可能性あり）"}',
        f'既存ファイル: {"スキップ" if skip_existing else "上書き"}',
        ''
    ]

    success_count = 0
    skip_count = 0
    error_count = 0
    total_downloaded_size = 0

    for idx, url in enumerate(urls, 1):
        progress((idx - 1) / len(urls), f"処理中 {idx}/{len(urls)}")
        
        logs.append(f'[{idx:03d}/{len(urls):03d}] 開始: {url}')
        
        try:
            # ファイル名の事前取得（HEADリクエストで効率化）
            fname = os.path.basename(url.split('?')[0]) or f'file_{idx}'
            out_path = os.path.join(dest_dir, fname)
            
            # 既存ファイルの確認（詳細チェック）
            if skip_existing and os.path.exists(out_path):
                existing_size = os.path.getsize(out_path)
                # ファイルサイズが0の場合は破損とみなして再ダウンロード
                if existing_size > 0:
                    logs.append(f'  -> スキップ: ファイル既存 ({format_file_size(existing_size)}) {fname}')
                    skip_count += 1
                    continue
                else:
                    logs.append(f'  -> 再ダウンロード: 既存ファイルが破損 (0バイト) {fname}')
            
            # より正確なファイル名取得のためのHEADリクエスト
            logs.append(f'  -> ファイル情報取得中...')
            try:
                head_resp = requests.head(url, headers=headers, timeout=30)
                if head_resp.status_code == 200:
                    cd = head_resp.headers.get('content-disposition', '')
                    if cd:
                        fname = get_filename_from_cd(cd) or fname
                        out_path = os.path.join(dest_dir, fname)
                        
                        # HEADリクエスト後の再チェック
                        if skip_existing and os.path.exists(out_path):
                            existing_size = os.path.getsize(out_path)
                            if existing_size > 0:
                                logs.append(f'  -> スキップ: ファイル既存 ({format_file_size(existing_size)}) {fname}')
                                skip_count += 1
                                continue
            except:
                logs.append(f'  -> HEADリクエスト失敗、通常のGETリクエストで継続')
            
            # リクエスト開始
            logs.append(f'  -> ダウンロード開始: {fname}')
            request_start = time.time()
            
            resp = requests.get(url, headers=headers, stream=True, timeout=60)
            resp.raise_for_status()
            
            request_time = time.time() - request_start
            logs.append(f'  -> レスポンス取得: {resp.status_code} ({request_time:.1f}秒)')
            
            # ファイル名の最終決定
            cd = resp.headers.get('content-disposition', '')
            final_fname = get_filename_from_cd(cd) or fname
            final_out_path = os.path.join(dest_dir, final_fname)
            
            # 最終ファイル名での重複チェック
            if skip_existing and final_out_path != out_path and os.path.exists(final_out_path):
                existing_size = os.path.getsize(final_out_path)
                if existing_size > 0:
                    logs.append(f'  -> スキップ: 最終ファイル名で既存確認 ({format_file_size(existing_size)}) {final_fname}')
                    skip_count += 1
                    continue
            
            # 上書きの場合の警告
            if not skip_existing and os.path.exists(final_out_path):
                existing_size = os.path.getsize(final_out_path)
                logs.append(f'  -> 上書き: 既存ファイル ({format_file_size(existing_size)}) {final_fname}')
            
            out_path = final_out_path
            fname = final_fname
            
            # Content-Lengthからファイルサイズを取得
            content_length = resp.headers.get('content-length')
            expected_size = int(content_length) if content_length else None
            size_info = f" ({format_file_size(expected_size)})" if expected_size else ""
            
            logs.append(f'  -> ダウンロード開始: {fname}{size_info}')
            
            # ファイルダウンロード
            download_start = time.time()
            downloaded_size = 0
            
            with open(out_path, 'wb') as f:
                for chunk in resp.iter_content(8192):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
            
            download_time = time.time() - download_start
            actual_size = os.path.getsize(out_path)
            speed = actual_size / download_time if download_time > 0 else 0
            
            logs.append(f'  -> 完了: {format_file_size(actual_size)} ({format_duration(download_time)}, {format_file_size(speed)}/s)')
            
            # サイズ不一致の警告
            if expected_size and actual_size != expected_size:
                logs.append(f'  -> 警告: サイズ不一致 (期待:{format_file_size(expected_size)}, 実際:{format_file_size(actual_size)})')
            
            success_count += 1
            total_downloaded_size += actual_size
            
        except requests.exceptions.Timeout:
            logs.append(f'  -> [タイムアウト] 60秒以内にレスポンスが得られませんでした')
            error_count += 1
        except requests.exceptions.ConnectionError as e:
            logs.append(f'  -> [接続エラー] {str(e)}')
            error_count += 1
        except requests.exceptions.HTTPError as e:
            logs.append(f'  -> [HTTPエラー] {resp.status_code}: {str(e)}')
            error_count += 1
        except requests.exceptions.RequestException as e:
            logs.append(f'  -> [リクエストエラー] {str(e)}')
            error_count += 1
        except FileNotFoundError:
            logs.append(f'  -> [ファイルエラー] 保存先フォルダにアクセスできません: {dest_dir}')
            error_count += 1
        except PermissionError:
            logs.append(f'  -> [権限エラー] ファイルの書き込み権限がありません: {out_path}')
            error_count += 1
        except OSError as e:
            logs.append(f'  -> [OSエラー] {str(e)}')
            error_count += 1
        except Exception as e:
            logs.append(f'  -> [予期しないエラー] {type(e).__name__}: {str(e)}')
            error_count += 1
        
        logs.append('')  # 空行で区切り
    
    # 最終結果のサマリー
    total_time = time.time() - start_time
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
        logs.append('※ エラーが発生したURLがあります。上記ログで詳細を確認してください。')
    
    progress(1.0, "完了")
    return '\n'.join(logs)

# --- Gradio UI ---
def ui():
    with gr.Blocks(analytics_enabled=False) as demo:
        gr.Markdown('## URL Scooop: 一括ダウンロード')
        with gr.Row():
            txt_file = gr.File(label='URLリスト(.txt)', file_types=['.txt'])
            url_text = gr.Textbox(label='URLリスト(改行区切り)', lines=4)
        dest_dir = gr.Textbox(label='保存先フォルダ', placeholder='/path/to/output')
        skip_existing = gr.Checkbox(label='既存ファイルをスキップ', value=True)
        btn = gr.Button('ダウンロード開始')
        log_output = gr.Textbox(label='ログ', lines=20)
        btn.click(batch_download, [txt_file, url_text, dest_dir, skip_existing], log_output)
    return demo

# WebUI にタブ追加
script_callbacks.on_ui_tabs(lambda: [(ui(), 'URL Scooop', 'url_scooop')])