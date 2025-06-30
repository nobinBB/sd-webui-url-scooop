import os
import re
import requests
import gradio as gr
from modules import script_callbacks, shared

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

# --- ダウンロード処理 ---
def batch_download(txt_file, url_text, dest_dir):
    urls = read_urls(txt_file, url_text)
    if not urls:
        return 'URLが指定されていません。ファイルかテキストで入力してください。'
    if not dest_dir:
        return '保存先フォルダを指定してください。'
    os.makedirs(dest_dir, exist_ok=True)

    api_key = get_civitai_api_key()
    headers = {'Authorization': f'Bearer {api_key}'} if api_key else {}
    logs = [f'合計 {len(urls)} 件をダウンロード: {dest_dir}']
    if not api_key:
        logs.append('[WARN] API キー未設定: 認証エラーになる可能性があります。')

    for idx, url in enumerate(urls, 1):
        logs.append(f'[{idx}/{len(urls)}] {url}')
        try:
            resp = requests.get(url, headers=headers, stream=True, timeout=60)
            resp.raise_for_status()
            cd = resp.headers.get('content-disposition', '')
            fname = get_filename_from_cd(cd) or os.path.basename(url.split('?')[0]) or f'file_{idx}'
            out_path = os.path.join(dest_dir, fname)
            with open(out_path, 'wb') as f:
                for chunk in resp.iter_content(8192):
                    f.write(chunk)
            logs.append(f'  -> 保存: {out_path}')
        except Exception as e:
            logs.append(f'  [失敗] {e}')
    logs.append('ダウンロード完了')
    return '\n'.join(logs)

# --- Gradio UI ---
def ui():
    with gr.Blocks(analytics_enabled=False) as demo:
        gr.Markdown('## URL Scooop: 一括ダウンロード')
        with gr.Row():
            txt_file = gr.File(label='URLリスト(.txt)', file_types=['.txt'])
            url_text = gr.Textbox(label='URLリスト(改行区切り)', lines=4)
        dest_dir = gr.Textbox(label='保存先フォルダ', placeholder='/path/to/output')
        btn = gr.Button('ダウンロード開始')
        log_output = gr.Textbox(label='ログ', lines=20)
        btn.click(batch_download, [txt_file, url_text, dest_dir], log_output)
    return demo

# WebUI にタブ追加
script_callbacks.on_ui_tabs(lambda: [(ui(), 'URL Scooop', 'url_scooop')])
