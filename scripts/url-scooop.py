import os
import requests
import gradio as gr
from modules import script_callbacks, shared
import re

# Content-Dispositionヘッダーからファイル名を取得
def get_filename_from_cd(cd_header):
    if not cd_header:
        return None
    m = re.search(r"filename\*=[^']*''([^;\r\n]+)", cd_header)
    if m:
        filename = m.group(1)
        try:
            return requests.utils.unquote(filename)
        except:
            return filename
    m = re.search(r'filename="?([^";]+)"?', cd_header)
    if m:
        return m.group(1)
    return None

# アップロードされたtxtの内容を読み込む
def read_txt_file(txt_file):
    path = getattr(txt_file, 'name', None)
    if isinstance(path, str) and os.path.isfile(path):
        return open(path, 'r', encoding='utf-8').read()
    if isinstance(txt_file, dict):
        path = txt_file.get('name') or txt_file.get('file_name')
        if isinstance(path, str) and os.path.isfile(path):
            try:
                return open(path, 'r', encoding='utf-8').read()
            except:
                pass
        data = txt_file.get('data')
        if isinstance(data, (bytes, bytearray)):
            return data.decode('utf-8')
        if isinstance(data, str) and '\n' in data:
            return data
    if hasattr(txt_file, 'read') and callable(txt_file.read):
        try:
            data = txt_file.read()
            if isinstance(data, (bytes, bytearray)):
                return data.decode('utf-8')
            return data
        except:
            pass
    return None

# Civitai Helperから API Key を取得
def get_civitai_api_key():
    key = shared.opts.data.get('ch_civiai_api_key', None)
    if not key:
        key = os.getenv('CIVITAI_API_KEY') or os.getenv('STABLEDIFFUSION_CIVITAI_API_KEY')
    return key

# 一括ダウンロード処理
def batch_download(txt_file, url_text, dest_dir):
    urls, logs = [], []
    # ファイル入力からURLを抽出
    if txt_file is not None:
        content = read_txt_file(txt_file)
        if not content:
            return 'ファイル読み込みに失敗しました。'
        urls += [u.strip() for u in content.splitlines() if u.strip()]
    # テキスト入力からURLを抽出
    if url_text:
        urls += [u.strip() for u in url_text.splitlines() if u.strip()]
    if not urls:
        return 'URLが指定されていません。'
    if not dest_dir:
        return '保存先フォルダを指定してください。'
    os.makedirs(dest_dir, exist_ok=True)

    # 認証設定
    api_key = get_civitai_api_key()
    headers = {}
    logs.append(f'合計 {len(urls)} 件をダウンロード: {dest_dir}')
    if api_key:
        headers['Authorization'] = f'Bearer {api_key}'
    else:
        logs.append('[WARN] API キーが設定されていません。401 エラーになります。')

    # ダウンロードループ
    for i, url in enumerate(urls, 1):
        logs.append(f'[{i}/{len(urls)}] {url}')
        try:
            resp = requests.get(url, headers=headers, stream=True, timeout=60)
            resp.raise_for_status()
            cd = resp.headers.get('content-disposition')
            fname = get_filename_from_cd(cd) or os.path.basename(url.split('?')[0]) or f'file_{i}'
            out_path = os.path.join(dest_dir, fname)
            with open(out_path, 'wb') as f:
                for chunk in resp.iter_content(8192):
                    f.write(chunk)
            logs.append(f'  -> 保存: {out_path}')
        except Exception as e:
            logs.append(f'  [失敗] {e}')
    logs.append('ダウンロード完了')
    return '\n'.join(logs)

# UI定義
def ui():
    with gr.Blocks(analytics_enabled=False) as demo:
        gr.Markdown('## URL Scooop: 一括ダウンロード')
        with gr.Row():
            txt_file = gr.File(label='URLリストファイル (.txt)', file_types=['.txt'])
            url_text = gr.Textbox(label='URLリスト (改行区切り)', lines=4)
        dest_dir = gr.Textbox(label='保存先フォルダ', placeholder='/path/to/models')
        btn = gr.Button('ダウンロード開始')
        out = gr.Textbox(label='ログ出力', lines=25)
        btn.click(batch_download, [txt_file, url_text, dest_dir], out)
    return demo

# WebUIタブ登録
script_callbacks.on_ui_tabs(lambda: [(ui(), 'URL Scooop', 'url_scooop')])
