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
    Georreferencia usando shape matching da linha da costa/agua entre planta e OSM tiles.
    """
    import numpy as np
    import cv2

    south, north, west, east = bbox

    # Baixar tiles OSM
    logs.append({'nivel': 'info', 'msg': '  Baixando mapa OSM (tiles)...'})
    img_osm = _baixar_tiles_osm(bbox, img_w, img_h, logs)

    # Salvar OSM tiles para referencia
    osm_path = f"/var/www/urbanlex/static/downloads/osm_tiles_{municipio.replace(' ','_')}.png"
    cv2.imwrite(osm_path, img_osm)

    # --- Extrair agua/costa da planta ---
    logs.append({'nivel': 'info', 'msg': '  Extraindo linha da costa da planta...'})
    costa_planta = _extrair_agua(img_planta, 'planta')

    # --- Extrair agua/costa do OSM ---
    logs.append({'nivel': 'info', 'msg': '  Extraindo linha da costa do OSM...'})
    costa_osm = _extrair_agua(img_osm, 'osm')

    # Salvar para debug
    cv2.imwrite(f"{tmp}/costa_planta.png", costa_planta)
    cv2.imwrite(f"{tmp}/costa_osm.png", costa_osm)

    n_planta = cv2.countNonZero(costa_planta)
    n_osm = cv2.countNonZero(costa_osm)
    logs.append({'nivel': 'info', 'msg': f'  Pixels agua: planta={n_planta}, osm={n_osm}'})

    if n_planta < 1000 or n_osm < 1000:
        logs.append({'nivel': 'aviso', 'msg': '  Agua insuficiente detectada — abortando'})
        return None

    # --- Extrair contornos da costa ---
    contornos_planta = _extrair_contornos_costa(costa_planta, img_w, img_h)
    contornos_osm = _extrair_contornos_costa(costa_osm, img_w, img_h)

    logs.append({'nivel': 'info', 'msg': f'  Contornos: planta={len(contornos_planta)}, osm={len(contornos_osm)}'})

    if not contornos_planta or not contornos_osm:
        logs.append({'nivel': 'aviso', 'msg': '  Contornos insuficientes'})
        return None

    # --- Shape matching com ECC (Enhanced Correlation Coefficient) ---
    logs.append({'nivel': 'info', 'msg': '  Alinhando contornos via ECC...'})

    # Usar o maior contorno de cada
    costa_p_bin = np.zeros((img_h, img_w), dtype=np.uint8)
    costa_o_bin = np.zeros((img_h, img_w), dtype=np.uint8)

    for cnt in contornos_planta[:3]:
        cv2.drawContours(costa_p_bin, [cnt], -1, 255, 3)
    for cnt in contornos_osm[:3]:
        cv2.drawContours(costa_o_bin, [cnt], -1, 255, 3)

    # ECC alignment
    warp_matrix = np.eye(2, 3, dtype=np.float32)
    criteria = (cv2.TERM_CRITERIA_EPS | cv2.TERM_CRITERIA_COUNT, 1000, 1e-6)

    try:
        costa_p_f = costa_p_bin.astype(np.float32) / 255.0
        costa_o_f = costa_o_bin.astype(np.float32) / 255.0

        # Tentar ECC com modelo afim
        cc, warp_matrix = cv2.findTransformECC(
            costa_o_f, costa_p_f,
            warp_matrix,
            cv2.MOTION_EUCLIDEAN,
            criteria,
            None, 5
        )
        logs.append({'nivel': 'ok', 'msg': f'  ECC convergiu: cc={cc:.4f}'})

    except Exception as e:
        logs.append({'nivel': 'aviso', 'msg': f'  ECC falhou: {str(e)[:80]} — tentando phase correlation'})

        # Fallback: phase correlation para translacao
        try:
            f1 = np.fft.fft2(costa_p_f)
            f2 = np.fft.fft2(costa_o_f)
            cross = f1 * np.conj(f2)
            cross /= (np.abs(cross) + 1e-10)
            corr = np.abs(np.fft.ifft2(cross))
            peak = np.unravel_index(np.argmax(corr), corr.shape)
            dy = peak[0] if peak[0] < img_h//2 else peak[0] - img_h
            dx = peak[1] if peak[1] < img_w//2 else peak[1] - img_w
            warp_matrix = np.array([[1, 0, float(dx)], [0, 1, float(dy)]], dtype=np.float32)
            logs.append({'nivel': 'ok', 'msg': f'  Phase correlation: dx={dx}, dy={dy}'})
        except Exception as e2:
            logs.append({'nivel': 'aviso', 'msg': f'  Phase correlation falhou: {str(e2)[:80]}'})
            return None

    # --- Gerar imagem de validacao ---
    planta_warp = cv2.warpAffine(img_planta, warp_matrix, (img_w, img_h))
    planta_warp_gray = cv2.cvtColor(planta_warp, cv2.COLOR_BGR2GRAY)
    _, planta_edges = cv2.threshold(planta_warp_gray, 30, 255, cv2.THRESH_BINARY)
    planta_edges = cv2.Canny(planta_warp_gray, 30, 90)

    val_img = np.zeros((img_h, img_w, 3), dtype=np.uint8)
    val_img[:,:,0] = img_osm[:,:,0] if len(img_osm.shape)==3 else img_osm  # azul = OSM
    # Desenhar bordas da planta em vermelho
    osm_gray = cv2.cvtColor(img_osm, cv2.COLOR_BGR2GRAY)
    osm_edges = cv2.Canny(osm_gray, 30, 90)
    val_img[:,:,0] = planta_edges   # azul = planta
    val_img[:,:,2] = osm_edges      # vermelho = OSM

    val_path = f"/var/www/urbanlex/static/downloads/validacao_{municipio.replace(' ','_')}.png"
    cv2.imwrite(val_path, val_img)
    logs.append({'nivel': 'ok', 'msg': '  Imagem de validacao gerada'})

    # Montar M_full 3x3
    M_full = np.eye(3, dtype=np.float32)
    M_full[:2, :] = warp_matrix

    def px_to_ll(x, y):
        lon = west + (x / img_w) * (east - west)
        lat = north - (y / img_h) * (north - south)
        return lat, lon

    return M_full, px_to_ll, val_path


def _extrair_agua(img, tipo):
    """Extrai mascara de agua da imagem."""
    import numpy as np
    import cv2

    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)

    if tipo == 'planta':
        # Planta: agua/lagoas sao azul claro ou ciano claro
        masks = [
            cv2.inRange(hsv, np.array([85,30,150]), np.array([115,180,255])),   # azul claro
            cv2.inRange(hsv, np.array([155,10,200]), np.array([180,60,255])),   # azul muito claro
            cv2.inRange(hsv, np.array([85,10,200]), np.array([115,50,255])),    # azul palido
        ]
    else:
        # OSM tiles: agua e azul claro caracteristico #aad3df
        masks = [
            cv2.inRange(hsv, np.array([90,20,170]), np.array([115,80,255])),    # agua OSM
            cv2.inRange(hsv, np.array([85,15,180]), np.array([120,60,255])),    # variacao
        ]

    mask = masks[0]
    for m in masks[1:]:
        mask = cv2.bitwise_or(mask, m)

    # Morfologia para limpar ruido
    kernel = np.ones((5,5), np.uint8)
    mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, kernel, iterations=2)
    mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, kernel, iterations=1)

    return mask


def _extrair_contornos_costa(mask, img_w, img_h):
    """Extrai contornos ordenados por area."""
    import cv2
    contornos, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    min_area = img_w * img_h * 0.001
    contornos = [c for c in contornos if cv2.contourArea(c) > min_area]
    contornos = sorted(contornos, key=cv2.contourArea, reverse=True)
    return contornos



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
