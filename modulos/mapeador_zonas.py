"""
Modulo de mapeamento de zonas urbanisticas.
Pipeline: Gemini Vision (legenda) -> OSM -> Gemini Vision (georreferenciamento) -> OpenCV (segmentacao) -> KML
"""
import os
import json


def mapear_zonas(fpath, fname, municipio, estado, logs, job, tmp):
    """Pipeline principal de mapeamento de zonas."""
    import cv2
    resultado = {
        'legenda_ok': False, 'osm_ok': False, 'geo_ok': False,
        'zonas_ok': False, 'kml_ok': False, 'zonas': [], 'kml_url': None
    }
    try:
        # Estagio 1: Extrair legenda
        logs.append({'nivel': 'info', 'msg': '\U0001f50d Estagio 1/5: Extraindo legenda via Gemini Vision...'})
        legenda = _extrair_legenda(fpath, fname, municipio, estado, logs, tmp)
        if legenda:
            resultado['legenda_ok'] = True
            resultado['zonas'] = legenda
            logs.append({'nivel': 'ok', 'msg': f'\u2705 Legenda extraida: {len(legenda)} zonas identificadas'})
            for z in legenda:
                logs.append({'nivel': 'info', 'msg': f'  \U0001f4cd {z["nome"]} \u2014 cor: {z["cor_hex"]}'})
        else:
            logs.append({'nivel': 'aviso', 'msg': '\u26a0\ufe0f Nao foi possivel extrair a legenda'})
            job['result'] = resultado
            return

        # Estagio 2: Buscar OSM
        logs.append({'nivel': 'info', 'msg': f'\U0001f310 Estagio 2/5: Buscando eixos viarios de {municipio}/{estado} no OpenStreetMap...'})
        osm_data = _buscar_osm(municipio, estado, logs)
        if osm_data:
            resultado['osm_ok'] = True
            logs.append({'nivel': 'ok', 'msg': f'\u2705 OSM: {len(osm_data.get("elements", []))} elementos viarios encontrados'})
        else:
            logs.append({'nivel': 'aviso', 'msg': '\u26a0\ufe0f Nao foi possivel obter dados OSM'})
            job['result'] = resultado
            return

        # Carregar imagem da planta
        ext = os.path.splitext(fname)[1].lower()
        if ext == '.pdf':
            pages = sorted([x for x in os.listdir(tmp) if x.startswith('mapa_') and x.endswith('.png')])
            img_path = os.path.join(tmp, pages[0]) if pages else None
        else:
            img_path = fpath
        if not img_path or not os.path.exists(img_path):
            logs.append({'nivel': 'aviso', 'msg': '\u26a0\ufe0f Imagem da planta nao encontrada'})
            job['result'] = resultado
            return
        img_planta = cv2.imread(img_path)
        img_h, img_w = img_planta.shape[:2]
        _MAX_W = 2000
        if img_w > _MAX_W:
            _scale = _MAX_W / img_w
            img_planta = cv2.resize(img_planta, (int(img_w * _scale), int(img_h * _scale)), interpolation=cv2.INTER_AREA)
            img_h, img_w = img_planta.shape[:2]
            logs.append({'nivel': 'info', 'msg': f'  \U0001f4d0 Imagem redimensionada para {img_w}x{img_h}px'})
        logs.append({'nivel': 'info', 'msg': f'  \U0001f4cf Dimensoes: {img_w}x{img_h}px'})

        # Estagio 3: Georreferenciamento via Gemini Vision
        logs.append({'nivel': 'info', 'msg': '\U0001f916 Estagio 3/5: Gemini Vision identificando vias na planta...'})
        bbox_tuple = (float(osm_data.get('_south', -29.86)), float(osm_data.get('_north', -29.76)),
                      float(osm_data.get('_west', -50.12)), float(osm_data.get('_east', -50.02)))
        geo_result = _georreferenciar_gemini(img_planta, osm_data, bbox_tuple, img_w, img_h, municipio, estado, logs, tmp)
        if not geo_result:
            logs.append({'nivel': 'aviso', 'msg': '\u26a0\ufe0f Georreferenciamento falhou'})
            job['result'] = resultado
            return
        H, px_to_ll, val_pontos_path = geo_result
        resultado['geo_ok'] = True
        resultado['validacao_pontos_url'] = f"/static/downloads/validacao_pontos_{municipio.replace(' ', '_')}.png"
        resultado['osm_tiles_url'] = f"/static/downloads/osm_tiles_{municipio.replace(' ', '_')}.png"

        # Gerar validacao
        import numpy as _np
        img_vias_osm = _renderizar_osm(osm_data, bbox_tuple, img_w, img_h)
        _val = _np.zeros((img_h, img_w, 3), dtype=_np.uint8)
        _pw = cv2.warpAffine(img_planta, H[:2, :], (img_w, img_h))
        _pw_gray = cv2.cvtColor(_pw, cv2.COLOR_BGR2GRAY)
        # Usar Canny para extrair bordas da planta (evita fundo branco sólido)
        _pw_edges = cv2.Canny(_pw_gray, 30, 90)
        _val[:, :, 0] = img_vias_osm   # azul = OSM (referencia)
        _val[:, :, 2] = _pw_edges      # vermelho = bordas da planta transformada
        _val_path = f"/var/www/urbanlex/static/downloads/validacao_{municipio.replace(' ', '_')}.png"
        cv2.imwrite(_val_path, _val)
        resultado['validacao_url'] = f"/static/downloads/validacao_{municipio.replace(' ', '_')}.png"
        logs.append({'nivel': 'ok', 'msg': '\u2705 Georreferenciamento concluido \u2014 validacao disponivel'})

        # Estagio 4: Segmentacao
        logs.append({'nivel': 'info', 'msg': '\U0001f3a8 Estagio 4/5: Segmentando zonas por cor...'})
        zonas_geo = _segmentar_zonas(img_planta, legenda, H, px_to_ll, logs)
        if zonas_geo:
            resultado['zonas_ok'] = True
            resultado['zonas'] = [{'nome': z['nome'], 'descricao': z.get('descricao', ''), 'cor': z['cor_hex'], 'area_km2': z.get('area_km2', '\u2014')} for z in zonas_geo]
            logs.append({'nivel': 'ok', 'msg': f'\u2705 {len(zonas_geo)} poligonos segmentados'})

        # Estagio 5: KML
        logs.append({'nivel': 'info', 'msg': '\U0001f4e6 Estagio 5/5: Gerando KML...'})
        kml_path = f"/var/www/urbanlex/static/downloads/zoneamento_{municipio.replace(' ', '_')}.kml"
        if zonas_geo:
            _gerar_kml(zonas_geo, municipio, estado, kml_path)
            resultado['kml_ok'] = True
            resultado['kml_url'] = f"/static/downloads/zoneamento_{municipio.replace(' ', '_')}.kml"
            logs.append({'nivel': 'ok', 'msg': '\u2705 KML gerado com sucesso!'})

        job['result'] = resultado
    except Exception as e:
        import traceback
        logs.append({'nivel': 'erro', 'msg': f'Erro: {str(e)[:200]}'})
        logs.append({'nivel': 'erro', 'msg': traceback.format_exc()[:500]})
        job['result'] = resultado
    finally:
        job['done'] = True


def _extrair_legenda(fpath, fname, municipio, estado, logs, tmp):
    """Extrai zonas e cores da legenda via Gemini Vision."""
    import re
    import json as _j
    from google import genai as _gv
    from google.genai import types as _gv_types
    import concurrent.futures as _cf
    import mimetypes
    try:
        client = _gv.Client(api_key=os.environ.get('GEMINI_API_KEY', ''))
        ext = os.path.splitext(fname)[1].lower()
        if ext == '.pdf':
            import subprocess
            subprocess.run(['gs', '-dNOPAUSE', '-dBATCH', '-sDEVICE=png16m', '-r150',
                            f'-sOutputFile={tmp}/mapa_%03d.png', fpath], capture_output=True, timeout=120)
            pages = sorted([x for x in os.listdir(tmp) if x.startswith('mapa_') and x.endswith('.png')])
            if not pages:
                return None
            img_path = os.path.join(tmp, pages[0])
        else:
            img_path = fpath
        mime = mimetypes.guess_type(img_path)[0] or 'image/png'
        with open(img_path, 'rb') as fp:
            img_bytes = fp.read()
        prompt = (
            f"Esta e uma planta de zoneamento municipal de {municipio}/{estado}.\n"
            f"Analise a LEGENDA e liste TODAS as zonas/subzonas presentes.\n"
            f"Para cada zona: nome/codigo, descricao completa, cor em HEX.\n"
            f"Responda APENAS com JSON valido, sem texto adicional:\n"
            f'[{{"nome":"ZR1","descricao":"Zona Residencial 1","cor_hex":"#FFD700"}}]'
        )
        parts = [_gv_types.Part.from_text(text=prompt),
                 _gv_types.Part.from_bytes(data=img_bytes, mime_type=mime)]
        ex = _cf.ThreadPoolExecutor(max_workers=1)
        fut = ex.submit(client.models.generate_content, model='gemini-2.5-flash', contents=parts)
        try:
            resp = fut.result(timeout=120)
            ex.shutdown(wait=False)
        except _cf.TimeoutError:
            ex.shutdown(wait=False)
            return None
        if not resp or not resp.text:
            return None
        txt = re.sub(r'^```json\s*|\s*```$', '', resp.text.strip())
        return _j.loads(txt)
    except Exception as e:
        logs.append({'nivel': 'aviso', 'msg': f'Erro ao extrair legenda: {str(e)[:100]}'})
        return None


def _buscar_osm(municipio, estado, logs):
    """Busca eixos viarios do municipio via Overpass API com cache local."""
    import requests
    import hashlib as _hlib
    import json as _jc
    try:
        r = requests.get("https://nominatim.openstreetmap.org/search",
                         params={"q": f"{municipio}, {estado}, Brasil", "format": "json", "limit": 1},
                         headers={"User-Agent": "UrbanLex/1.0"}, timeout=15)
        results = r.json()
        if not results:
            logs.append({"nivel": "aviso", "msg": "  Municipio nao encontrado no OSM"})
            return None
        bbox = results[0].get("boundingbox", [])
        if len(bbox) < 4:
            return None
        south, north, west, east = bbox[0], bbox[1], bbox[2], bbox[3]
        logs.append({"nivel": "info", "msg": f"  \U0001f4cc Bbox: {south},{west} \u2192 {north},{east}"})

        _cache_key = _hlib.md5(f"{municipio}{estado}{south}{north}{west}{east}".encode()).hexdigest()[:12]
        _cache_path = f"/var/www/urbanlex/static/downloads/osm_cache_{_cache_key}.json"
        if os.path.exists(_cache_path):
            logs.append({"nivel": "info", "msg": "  Cache OSM local encontrado"})
            with open(_cache_path, "r") as _cf:
                data = _jc.load(_cf)
            logs.append({"nivel": "info", "msg": f"  {len(data.get('elements', []))} vias do cache"})
            return data

        _servers = [
            "https://overpass-api.de/api/interpreter",
            "https://overpass.kumi.systems/api/interpreter",
            "https://overpass.openstreetmap.fr/api/interpreter",
            "https://overpass.private.coffee/api/interpreter",
            "https://overpass.osm.ch/api/interpreter",
        ]
        query = f'[out:json][timeout:30];(way["highway"~"primary|secondary|tertiary|residential|trunk"]({south},{west},{north},{east}););out geom;'
        data = None
        import time as _t
        for _srv in _servers:
            try:
                logs.append({"nivel": "info", "msg": f"  Tentando: {_srv.split('/')[2]}..."})
                _t.sleep(2)
                r2 = requests.post(_srv, data={"data": query}, timeout=60)
                if r2.status_code == 200 and r2.text.strip().startswith("{"):
                    data = r2.json()
                    if data.get("elements"):
                        break
                    logs.append({"nivel": "aviso", "msg": f"  0 elementos em {_srv.split('/')[2]}"})
                else:
                    logs.append({"nivel": "aviso", "msg": f"  Servidor indisponivel: {_srv.split('/')[2]}"})
            except Exception as _se:
                logs.append({"nivel": "aviso", "msg": f"  Erro {_srv.split('/')[2]}: {str(_se)[:60]}"})

        if not data or not data.get("elements"):
            return None

        logs.append({"nivel": "info", "msg": f"  {len(data.get('elements', []))} vias obtidas"})
        data["_south"] = float(south)
        data["_north"] = float(north)
        data["_west"] = float(west)
        data["_east"] = float(east)

        with open(_cache_path, "w") as _cf:
            _jc.dump(data, _cf)
        return data
    except Exception as e:
        logs.append({"nivel": "aviso", "msg": f"Erro OSM: {str(e)[:100]}"})
        return None



def _baixar_tiles_osm(bbox, img_w, img_h, logs):
    """Baixa tiles do OSM e monta imagem de mapa para o bbox do municipio."""
    import requests
    import math
    import numpy as np
    import cv2
    from PIL import Image
    import io

    south, north, west, east = bbox

    # Calcular zoom ideal
    def lat_to_tile_y(lat, zoom):
        lat_r = math.radians(lat)
        n = 2 ** zoom
        return int((1 - math.log(math.tan(lat_r) + 1/math.cos(lat_r)) / math.pi) / 2 * n)

    def lon_to_tile_x(lon, zoom):
        n = 2 ** zoom
        return int((lon + 180) / 360 * n)

    def tile_to_lon(x, zoom):
        return x / (2 ** zoom) * 360 - 180

    def tile_to_lat(y, zoom):
        n = math.pi - 2 * math.pi * y / (2 ** zoom)
        return math.degrees(math.atan(math.sinh(n)))

    # Zoom 14 para municipios pequenos
    zoom = 14

    x_min = lon_to_tile_x(west, zoom)
    x_max = lon_to_tile_x(east, zoom)
    y_min = lat_to_tile_y(north, zoom)
    y_max = lat_to_tile_y(south, zoom)

    n_tiles_x = x_max - x_min + 1
    n_tiles_y = y_max - y_min + 1
    logs.append({'nivel': 'info', 'msg': f'  Baixando {n_tiles_x}x{n_tiles_y} tiles OSM zoom={zoom}...'})

    # Montar imagem de tiles (256x256 cada)
    tile_size = 256
    canvas = Image.new('RGB', (n_tiles_x * tile_size, n_tiles_y * tile_size), (240, 240, 240))

    servers = [
        'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
        'https://a.tile.openstreetmap.org/{z}/{x}/{y}.png',
        'https://b.tile.openstreetmap.org/{z}/{x}/{y}.png',
    ]

    headers = {'User-Agent': 'UrbanLex/1.0 (georreferenciamento@urbanlex.com.br)'}

    for tx in range(x_min, x_max + 1):
        for ty in range(y_min, y_max + 1):
            for srv in servers:
                try:
                    url = srv.format(z=zoom, x=tx, y=ty)
                    r = requests.get(url, headers=headers, timeout=10)
                    if r.status_code == 200:
                        tile_img = Image.open(io.BytesIO(r.content)).convert('RGB')
                        px = (tx - x_min) * tile_size
                        py = (ty - y_min) * tile_size
                        canvas.paste(tile_img, (px, py))
                        break
                except Exception:
                    pass

    # Recortar exatamente no bbox
    total_w = n_tiles_x * tile_size
    total_h = n_tiles_y * tile_size

    lon_min_tile = tile_to_lon(x_min, zoom)
    lon_max_tile = tile_to_lon(x_max + 1, zoom)
    lat_max_tile = tile_to_lat(y_min, zoom)
    lat_min_tile = tile_to_lat(y_max + 1, zoom)

    crop_x1 = int((west - lon_min_tile) / (lon_max_tile - lon_min_tile) * total_w)
    crop_x2 = int((east - lon_min_tile) / (lon_max_tile - lon_min_tile) * total_w)
    crop_y1 = int((lat_max_tile - north) / (lat_max_tile - lat_min_tile) * total_h)
    crop_y2 = int((lat_max_tile - south) / (lat_max_tile - lat_min_tile) * total_h)

    canvas = canvas.crop((crop_x1, crop_y1, crop_x2, crop_y2))

    # Redimensionar para mesmas dimensoes da planta
    canvas = canvas.resize((img_w, img_h), Image.LANCZOS)

    # Converter para numpy BGR
    img = cv2.cvtColor(np.array(canvas), cv2.COLOR_RGB2BGR)
    logs.append({'nivel': 'ok', 'msg': f'  Tiles OSM montados: {img_w}x{img_h}px'})
    return img





def _georreferenciar_gemini(img_planta, osm_data, bbox, img_w, img_h, municipio, estado, logs, tmp):
    """
    Georreferencia em 2 chamadas ao Gemini usando elementos lineares:
    1. Identifica linhas caracteristicas na planta (costa, rodovias, limites)
    2. Localiza as mesmas linhas no OSM com multiplos pontos
    """
    import numpy as np
    import cv2
    import re
    import json as _j
    import concurrent.futures as _cf
    from google import genai as _gv
    from google.genai import types as _gv_types

    south, north, west, east = bbox

    # Baixar tiles OSM
    logs.append({'nivel': 'info', 'msg': '  Baixando mapa OSM (tiles)...'})
    img_osm = _baixar_tiles_osm(bbox, img_w, img_h, logs)

    _planta_path = f"{tmp}/planta_orig.png"
    _osm_path = f"{tmp}/osm_tiles.png"
    cv2.imwrite(_planta_path, img_planta)
    cv2.imwrite(_osm_path, img_osm)

    osm_path = f"/var/www/urbanlex/static/downloads/osm_tiles_{municipio.replace(' ','_')}.png"
    cv2.imwrite(osm_path, img_osm)

    client = _gv.Client(api_key=os.environ.get('GEMINI_API_KEY', ''))

    # --- CHAMADA 1: identificar elementos lineares na planta ---
    logs.append({'nivel': 'info', 'msg': '  Chamada 1: Gemini identificando elementos lineares na planta...'})
    try:
        with open(_planta_path, 'rb') as fp:
            planta_bytes = fp.read()

        prompt1 = (
            f"Esta e uma planta de zoneamento municipal de {municipio}/{estado}, Brasil.\n\n"
            f"Identifique de 3 a 5 ELEMENTOS LINEARES que sejam facilmente reconheciveis "
            f"em qualquer outro mapa da mesma area. Exemplos:\n"
            f"- Linha da costa (beira do oceano)\n"
            f"- Rodovia principal (ex: ERS-389, Estrada do Mar)\n"
            f"- Margem de lagoa ou rio\n"
            f"- Limite municipal\n\n"
            f"Para cada elemento linear, forneça de 4 a 6 pontos ao longo da linha, "
            f"do inicio ao fim, em ordem sequencial.\n\n"
            f"Responda APENAS com JSON valido:\n"
            f'[{{\n'
            f'  "elemento": "Linha da costa (oceano)",\n'
            f'  "pontos": [\n'
            f'    {{"x": 10.5, "y": 82.3}},\n'
            f'    {{"x": 25.1, "y": 79.8}},\n'
            f'    {{"x": 40.2, "y": 76.1}},\n'
            f'    {{"x": 55.3, "y": 73.4}}\n'
            f'  ]\n'
            f'}}]\n\n'
            f"Coordenadas x,y em porcentagem da largura/altura da imagem (0 a 100).\n"
            f"IMPORTANTE: ignore a legenda no canto direito — foque apenas na area do mapa."
        )

        parts1 = [_gv_types.Part.from_text(text=prompt1),
                  _gv_types.Part.from_bytes(data=planta_bytes, mime_type='image/png')]

        ex = _cf.ThreadPoolExecutor(max_workers=1)
        fut = ex.submit(client.models.generate_content, model='gemini-2.5-flash', contents=parts1)
        try:
            resp1 = fut.result(timeout=120)
            ex.shutdown(wait=False)
        except _cf.TimeoutError:
            ex.shutdown(wait=False)
            logs.append({'nivel': 'aviso', 'msg': '  Gemini timeout (chamada 1)'})
            return None

        txt1 = re.sub(r'^```json\s*|\s*```$', '', resp1.text.strip())
        elementos = _j.loads(txt1)
        logs.append({'nivel': 'ok', 'msg': f'  {len(elementos)} elementos lineares identificados na planta'})
        for el in elementos:
            logs.append({'nivel': 'info', 'msg': f'    - {el["elemento"]} ({len(el["pontos"])} pontos)'})

    except Exception as e:
        logs.append({'nivel': 'aviso', 'msg': f'  Erro chamada 1: {str(e)[:100]}'})
        return None

    # --- CHAMADA 2: localizar os mesmos elementos no OSM ---
    logs.append({'nivel': 'info', 'msg': '  Chamada 2: Gemini localizando elementos no mapa OSM...'})
    try:
        with open(_osm_path, 'rb') as fp:
            osm_bytes = fp.read()

        lista_elementos = '\n'.join([f'{i+1}. {el["elemento"]}' for i, el in enumerate(elementos)])

        prompt2 = (
            f"Este e um mapa OpenStreetMap de {municipio}/{estado}, Brasil.\n\n"
            f"Localize os seguintes elementos lineares neste mapa e marque "
            f"de 4 a 6 pontos ao longo de cada um, em ordem sequencial do inicio ao fim:\n\n"
            f"{lista_elementos}\n\n"
            f"Responda APENAS com JSON valido:\n"
            f'[{{\n'
            f'  "id": 1,\n'
            f'  "elemento": "Linha da costa (oceano)",\n'
            f'  "encontrado": true,\n'
            f'  "pontos": [\n'
            f'    {{"x": 55.2, "y": 45.1}},\n'
            f'    {{"x": 62.3, "y": 52.4}},\n'
            f'    {{"x": 68.1, "y": 61.2}},\n'
            f'    {{"x": 74.5, "y": 70.8}}\n'
            f'  ]\n'
            f'}}]\n\n'
            f"Coordenadas x,y em porcentagem da largura/altura da imagem (0 a 100).\n"
            f"Se nao encontrar o elemento, coloque encontrado: false e pontos vazio."
        )

        parts2 = [_gv_types.Part.from_text(text=prompt2),
                  _gv_types.Part.from_bytes(data=osm_bytes, mime_type='image/png')]

        ex = _cf.ThreadPoolExecutor(max_workers=1)
        fut = ex.submit(client.models.generate_content, model='gemini-2.5-flash', contents=parts2)
        try:
            resp2 = fut.result(timeout=120)
            ex.shutdown(wait=False)
        except _cf.TimeoutError:
            ex.shutdown(wait=False)
            logs.append({'nivel': 'aviso', 'msg': '  Gemini timeout (chamada 2)'})
            return None

        txt2 = re.sub(r'^```json\s*|\s*```$', '', resp2.text.strip())
        localizacoes = _j.loads(txt2)

        # Montar pares de pontos correspondentes
        pontos_planta = []
        pontos_osm = []
        cores_debug = [(255,50,50),(50,255,50),(50,150,255),(255,200,0),(255,0,255)]

        for loc in localizacoes:
            if not loc.get('encontrado', False) or not loc.get('pontos'):
                logs.append({'nivel': 'aviso', 'msg': f'    Nao encontrado: {loc.get("elemento","?")}'})
                continue
            idx = loc['id'] - 1
            if idx < 0 or idx >= len(elementos):
                continue

            pts_planta = elementos[idx]['pontos']
            pts_osm = loc['pontos']
            n = min(len(pts_planta), len(pts_osm))

            logs.append({'nivel': 'info', 'msg': f'    OK {loc["elemento"]}: {n} pares de pontos'})

            for i in range(n):
                pontos_planta.append([pts_planta[i]['x']/100*img_w, pts_planta[i]['y']/100*img_h])
                pontos_osm.append([pts_osm[i]['x']/100*img_w, pts_osm[i]['y']/100*img_h])

        logs.append({'nivel': 'ok', 'msg': f'  Total: {len(pontos_planta)} pares de pontos correspondentes'})

    except Exception as e:
        logs.append({'nivel': 'aviso', 'msg': f'  Erro chamada 2: {str(e)[:100]}'})
        return None

    if len(pontos_planta) < 4:
        logs.append({'nivel': 'aviso', 'msg': f'  Pontos insuficientes ({len(pontos_planta)}) — minimo 4'})
        return None

    # --- Calcular transformacao afim ---
    src_pts = np.array(pontos_planta, dtype=np.float32)
    dst_pts = np.array(pontos_osm, dtype=np.float32)

    M, inliers = cv2.estimateAffinePartial2D(src_pts, dst_pts, method=cv2.RANSAC, ransacReprojThreshold=20.0)
    if M is None:
        logs.append({'nivel': 'aviso', 'msg': '  Falha ao calcular transformacao afim'})
        return None

    inliers_count = int(inliers.sum()) if inliers is not None else 0
    scale = np.sqrt(M[0,0]**2 + M[1,0]**2)
    logs.append({'nivel': 'ok', 'msg': f'  Transformacao: escala={scale:.3f} inliers={inliers_count}/{len(pontos_planta)}'})

    # --- Gerar imagem de validacao lado a lado com linhas ---
    planta_vis = img_planta.copy()
    osm_vis = img_osm.copy()

    cor_idx = 0
    for loc in localizacoes:
        if not loc.get('encontrado') or not loc.get('pontos'):
            continue
        idx = loc['id'] - 1
        if idx < 0 or idx >= len(elementos):
            continue
        cor = cores_debug[cor_idx % len(cores_debug)]
        cor_idx += 1

        pts_planta = elementos[idx]['pontos']
        pts_osm = loc['pontos']

        # Desenhar linha na planta
        for i in range(len(pts_planta)-1):
            p1 = (int(pts_planta[i]['x']/100*img_w), int(pts_planta[i]['y']/100*img_h))
            p2 = (int(pts_planta[i+1]['x']/100*img_w), int(pts_planta[i+1]['y']/100*img_h))
            cv2.line(planta_vis, p1, p2, cor, 4)
        for pt in pts_planta:
            cv2.circle(planta_vis, (int(pt['x']/100*img_w), int(pt['y']/100*img_h)), 10, cor, -1)

        # Desenhar linha no OSM
        for i in range(len(pts_osm)-1):
            p1 = (int(pts_osm[i]['x']/100*img_w), int(pts_osm[i]['y']/100*img_h))
            p2 = (int(pts_osm[i+1]['x']/100*img_w), int(pts_osm[i+1]['y']/100*img_h))
            cv2.line(osm_vis, p1, p2, cor, 4)
        for pt in pts_osm:
            cv2.circle(osm_vis, (int(pt['x']/100*img_w), int(pt['y']/100*img_h)), 10, cor, -1)

    sep = np.full((img_h, 20, 3), 60, dtype=np.uint8)
    val_img = np.hstack([planta_vis, sep, osm_vis])
    val_path = f"/var/www/urbanlex/static/downloads/validacao_pontos_{municipio.replace(' ','_')}.png"
    cv2.imwrite(val_path, val_img)
    logs.append({'nivel': 'ok', 'msg': '  Imagem de validacao gerada — confira as linhas'})

    M_full = np.eye(3, dtype=np.float32)
    M_full[:2, :] = M

    def px_to_ll(x, y):
        lon = west + (x / img_w) * (east - west)
        lat = north - (y / img_h) * (north - south)
        return lat, lon

    return M_full, px_to_ll, val_path



def _renderizar_osm(osm_data, bbox, img_w, img_h):
    """Renderiza eixos viarios OSM como imagem numpy."""
    import numpy as np
    import cv2
    south, north, west, east = bbox
    img = np.zeros((img_h, img_w), dtype=np.uint8)

    def ll_to_px(lat, lon):
        x = int((lon - west) / (east - west) * img_w)
        y = int((north - lat) / (north - south) * img_h)
        return (x, y)

    for el in osm_data.get('elements', []):
        if el.get('type') == 'way' and el.get('geometry'):
            pts = [ll_to_px(g['lat'], g['lon']) for g in el['geometry']]
            for i in range(len(pts) - 1):
                cv2.line(img, pts[i], pts[i + 1], 255, 2)
    return img


def _segmentar_zonas(img_planta, legenda, H, px_to_ll, logs):
    """Segmenta zonas por cor e converte para poligonos georreferenciados."""
    import numpy as np
    import cv2
    from shapely.geometry import Polygon
    zonas_geo = []
    img_h, img_w = img_planta.shape[:2]
    for zona in legenda:
        nome = zona.get('nome', '')
        cor_hex = zona.get('cor_hex', '#000000')
        try:
            r = int(cor_hex[1:3], 16)
            g = int(cor_hex[3:5], 16)
            b = int(cor_hex[5:7], 16)
        except Exception:
            continue
        target = np.array([b, g, r], dtype=np.uint8)
        lower = np.clip(target.astype(int) - 25, 0, 255).astype(np.uint8)
        upper = np.clip(target.astype(int) + 25, 0, 255).astype(np.uint8)
        mask = cv2.inRange(img_planta, lower, upper)
        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        if not contours:
            continue
        min_area = img_w * img_h * 0.0001
        contours = [c for c in contours if cv2.contourArea(c) > min_area]
        if not contours:
            continue
        logs.append({'nivel': 'info', 'msg': f'  {nome}: {len(contours)} poligono(s)'})
        for cnt in contours:
            pts = cnt.reshape(-1, 1, 2).astype(np.float32)
            pts_t = cv2.perspectiveTransform(pts, H)
            pts_ll = [px_to_ll(p[0][0], p[0][1]) for p in pts_t]
            if len(pts_ll) >= 3:
                zonas_geo.append({
                    'nome': nome,
                    'descricao': zona.get('descricao', ''),
                    'cor_hex': cor_hex,
                    'coordenadas': pts_ll,
                    'area_km2': round(Polygon([(p[1], p[0]) for p in pts_ll]).area * 111 * 111, 4)
                })
    logs.append({'nivel': 'ok', 'msg': f'  {len(zonas_geo)} poligonos georreferenciados'})
    return zonas_geo


def _gerar_kml(zonas_geo, municipio, estado, output_path):
    """Gera arquivo KML com as zonas."""
    import simplekml
    kml = simplekml.Kml()
    kml.document.name = f'Zoneamento {municipio}/{estado}'
    for zona in zonas_geo:
        pol = kml.newpolygon(name=zona['nome'])
        coords = [(lon, lat) for lat, lon in zona['coordenadas']]
        if coords[0] != coords[-1]:
            coords.append(coords[0])
        pol.outerboundaryis = coords
        try:
            hex_c = zona['cor_hex'].lstrip('#')
            r, g, b = int(hex_c[0:2], 16), int(hex_c[2:4], 16), int(hex_c[4:6], 16)
            pol.style.polystyle.color = simplekml.Color.rgb(r, g, b, a=180)
            pol.style.linestyle.color = simplekml.Color.rgb(r, g, b)
            pol.style.linestyle.width = 1
        except Exception:
            pass
        pol.description = f'{zona.get("descricao", "")} — {zona.get("area_km2", "?")} km2'
    kml.save(output_path)
    return output_path
