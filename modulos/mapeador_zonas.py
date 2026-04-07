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
                            '-dFirstPage=1', '-dLastPage=1',
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
    """Baixa imagem do Google Maps Static API para o bbox do municipio."""
    import requests
    import numpy as np
    import cv2
    from PIL import Image
    import io
    import os
    import math

    south, north, west, east = bbox

    GMAPS_KEY = os.environ.get('GOOGLE_MAPS_KEY', 'AIzaSyCuiZTfrnvUC-1X_suD3w6iGVyT_bhdVpQ')

    # Centro do bbox
    center_lat = (south + north) / 2
    center_lon = (west + east) / 2

    # Calcular zoom ideal para cobrir o bbox
    def get_zoom(south, north, west, east, img_w, img_h):
        WORLD_DIM = 256
        lat_rad = lambda lat: math.radians(lat)
        lat_to_mercator = lambda lat: math.log(math.tan(math.pi/4 + lat_rad(lat)/2))

        lat_fraction = (lat_to_mercator(north) - lat_to_mercator(south)) / (2 * math.pi)
        lon_fraction = (east - west) / 360.0

        lat_zoom = math.log2(img_h / WORLD_DIM / lat_fraction) if lat_fraction > 0 else 15
        lon_zoom = math.log2(img_w / WORLD_DIM / lon_fraction) if lon_fraction > 0 else 15

        return max(1, min(20, int(min(lat_zoom, lon_zoom))))

    zoom = get_zoom(south, north, west, east, img_w, img_h) - 1  # -1 para garantir cobertura total
    zoom = max(1, min(20, zoom))
    logs.append({'nivel': 'info', 'msg': f'  Google Maps Static: zoom={zoom}, centro=({center_lat:.4f},{center_lon:.4f})'})

    # Google Maps Static API — max 640x640 na versão gratuita, 2048x2048 no plano pago
    # Usar 640x640 e redimensionar depois
    size = 640

    url = (
        f"https://maps.googleapis.com/maps/api/staticmap"
        f"?center={center_lat},{center_lon}"
        f"&zoom={zoom}"
        f"&size={size}x{size}"
        f"&maptype=roadmap"
        f"&scale=2"
        f"&key={GMAPS_KEY}"
    )

    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            logs.append({'nivel': 'aviso', 'msg': f'  Google Maps erro {r.status_code}'})
            return None

        img_pil = Image.open(io.BytesIO(r.content)).convert('RGB')
        img = cv2.cvtColor(np.array(img_pil), cv2.COLOR_RGB2BGR)

        # Redimensionar para as dimensoes da planta
        img = cv2.resize(img, (img_w, img_h), interpolation=cv2.INTER_AREA)
        logs.append({'nivel': 'ok', 'msg': f'  Google Maps carregado: {img_w}x{img_h}px'})
        return img

    except Exception as e:
        logs.append({'nivel': 'aviso', 'msg': f'  Erro Google Maps: {str(e)[:80]}'})
        return None



def _georreferenciar_gemini(img_planta, osm_data, bbox, img_w, img_h, municipio, estado, logs, tmp):
    """
    Georreferencia em 2 chamadas ao Gemini com hierarquia:
    1. Vias nomeadas (cruzamento por nome)
    2. Descricao geometrica (vias sem nome)
    3. Feicoes nao-viarias (costa, lagoas, limites)
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

    osm_tiles_path = f"/var/www/urbanlex/static/downloads/osm_tiles_{municipio.replace(' ','_')}.png"
    cv2.imwrite(osm_tiles_path, img_osm)

    client = _gv.Client(api_key=os.environ.get('GEMINI_API_KEY', ''))

    # --- CHAMADA 1: identificar elementos na planta ---
    logs.append({'nivel': 'info', 'msg': '  Chamada 1: Gemini mapeando elementos na planta...'})
    try:
        with open(_planta_path, 'rb') as fp:
            planta_bytes = fp.read()

        prompt1 = (
            f"Esta e uma planta de zoneamento do municipio de {municipio}/{estado}, Brasil.\n\n"
            f"Identifique os seguintes tipos de elementos, em ordem de prioridade:\n\n"
            f"1. VIAS NOMEADAS: vias/rodovias com nome visivel no mapa\n"
            f"2. VIAS SEM NOME: vias principais identificaveis pela geometria (ex: 'via horizontal principal na parte inferior')\n"
            f"3. FEICOES NATURAIS: linha da costa, margens de lagoas/rios, limites municipais\n\n"
            f"Para cada elemento, marque 4 a 6 pontos ao longo dele em ordem sequencial.\n\n"
            f"Responda APENAS com JSON:\n"
            f'[{{\n'
            f'  "tipo": "via_nomeada",\n'
            f'  "nome": "ERS-389",\n'
            f'  "descricao": "rodovia horizontal na parte inferior do mapa",\n'
            f'  "pontos": [{{"x":15,"y":56}},{{"x":30,"y":56}},{{"x":45,"y":57}},{{"x":60,"y":57}}]\n'
            f'}},\n'
            f'{{\n'
            f'  "tipo": "feicao_natural",\n'
            f'  "nome": null,\n'
            f'  "descricao": "linha da costa do oceano atlantico",\n'
            f'  "pontos": [{{"x":10,"y":82}},{{"x":25,"y":80}},{{"x":40,"y":78}},{{"x":55,"y":76}}]\n'
            f'}}]\n\n'
            f"tipos validos: via_nomeada, via_sem_nome, feicao_natural\n"
            f"IMPORTANTE: ignore a legenda no canto direito.\n"
            f"MUITO IMPORTANTE: x e y sao PORCENTAGENS de 0 a 100, NAO pixels absolutos!\n"
            f"Exemplo: x=50.0 significa o centro horizontal da imagem."
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
        elementos_planta = _j.loads(txt1)

        n_nomeadas = sum(1 for e in elementos_planta if e.get('tipo') == 'via_nomeada')
        n_sem_nome = sum(1 for e in elementos_planta if e.get('tipo') == 'via_sem_nome')
        n_naturais = sum(1 for e in elementos_planta if e.get('tipo') == 'feicao_natural')
        logs.append({'nivel': 'ok', 'msg': f'  Planta: {n_nomeadas} vias nomeadas, {n_sem_nome} sem nome, {n_naturais} feicoes naturais'})
        for el in elementos_planta:
            logs.append({'nivel': 'info', 'msg': f'    [{el["tipo"]}] {el.get("nome") or el["descricao"][:50]} ({len(el["pontos"])} pts)'})

    except Exception as e:
        logs.append({'nivel': 'aviso', 'msg': f'  Erro chamada 1: {str(e)[:100]}'})
        return None

    # --- CHAMADA 2: localizar os mesmos elementos no OSM ---
    logs.append({'nivel': 'info', 'msg': '  Chamada 2: Gemini localizando elementos no OSM...'})
    try:
        with open(_osm_path, 'rb') as fp:
            osm_bytes = fp.read()

        # Montar lista de elementos para buscar
        lista = []
        for i, el in enumerate(elementos_planta):
            if el.get('nome'):
                desc = f'{i+1}. [{el["tipo"]}] Nome: "{el["nome"]}" — {el["descricao"]}'
            else:
                desc = f'{i+1}. [{el["tipo"]}] {el["descricao"]}'
            lista.append(desc)
        lista_str = '\n'.join(lista)

        prompt2 = (
            f"Este e um mapa OpenStreetMap do municipio de {municipio}/{estado}, Brasil.\n\n"
            f"Localize cada elemento abaixo neste mapa e marque 4 a 6 pontos ao longo dele:\n\n"
            f"{lista_str}\n\n"
            f"Para vias nomeadas: use o nome para localizar.\n"
            f"Para vias sem nome: use a descricao geometrica.\n"
            f"Para feicoes naturais: localize pelo tipo (costa, lagoa, etc).\n\n"
            f"Responda APENAS com JSON:\n"
            f'[{{\n'
            f'  "id": 1,\n'
            f'  "encontrado": true,\n'
            f'  "pontos": [{{"x":74,"y":8}},{{"x":71,"y":27}},{{"x":69,"y":50}},{{"x":67,"y":70}}]\n'
            f'}}]\n\n'
            f"MUITO IMPORTANTE: x e y sao PORCENTAGENS de 0 a 100, NAO pixels!\n"
            f"Exemplo correto: x=74.5 significa 74.5% da largura da imagem.\n"
            f"Se nao encontrar: encontrado: false e pontos: []"
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

        # Montar pares de pontos
        pontos_planta = []
        pontos_osm = []
        elementos_matched = []

        for loc in localizacoes:
            if not loc.get('encontrado') or not loc.get('pontos'):
                continue
            idx = loc['id'] - 1
            if idx < 0 or idx >= len(elementos_planta):
                continue
            el = elementos_planta[idx]
            pts_p = el['pontos']
            pts_o = loc['pontos']
            n = min(len(pts_p), len(pts_o))
            if n < 2:
                continue
            nome_el = el.get('nome') or el['descricao'][:30]
            logs.append({'nivel': 'info', 'msg': f'    OK [{el["tipo"]}] {nome_el}: {n} pares'})
            for i in range(n):
                # Normalizar: se valor > 100, assumir pixels absolutos e converter
                def norm(v, dim):
                    if v > 100:
                        return float(v) / dim * 100
                    return float(v)
                px = norm(pts_p[i]['x'], img_w) / 100 * img_w
                py = norm(pts_p[i]['y'], img_h) / 100 * img_h
                ox = norm(pts_o[i]['x'], img_w) / 100 * img_w
                oy = norm(pts_o[i]['y'], img_h) / 100 * img_h
                pontos_planta.append([px, py])
                pontos_osm.append([ox, oy])
            elementos_matched.append((el, loc, n))

        logs.append({'nivel': 'ok', 'msg': f'  Total: {len(pontos_planta)} pares de pontos'})

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

    # --- Gerar imagem de validacao lado a lado ---
    planta_vis = img_planta.copy()
    osm_vis = img_osm.copy()
    cores = [(255,50,50),(50,255,50),(50,150,255),(255,200,0),(255,0,255),(0,255,200),(255,128,0)]

    for i, (el, loc, n) in enumerate(elementos_matched):
        cor = cores[i % len(cores)]
        pts_p = el['pontos']
        pts_o = loc['pontos']
        n = min(len(pts_p), len(pts_o))

        for j in range(n-1):
            p1 = (int(pts_p[j]['x']/100*img_w), int(pts_p[j]['y']/100*img_h))
            p2 = (int(pts_p[j+1]['x']/100*img_w), int(pts_p[j+1]['y']/100*img_h))
            cv2.line(planta_vis, p1, p2, cor, 5)
            o1 = (int(pts_o[j]['x']/100*img_w), int(pts_o[j]['y']/100*img_h))
            o2 = (int(pts_o[j+1]['x']/100*img_w), int(pts_o[j+1]['y']/100*img_h))
            cv2.line(osm_vis, o1, o2, cor, 5)
        for pt in pts_p[:n]:
            cv2.circle(planta_vis, (int(pt['x']/100*img_w), int(pt['y']/100*img_h)), 12, cor, -1)
        for pt in pts_o[:n]:
            cv2.circle(osm_vis, (int(pt['x']/100*img_w), int(pt['y']/100*img_h)), 12, cor, -1)

    sep = np.full((img_h, 20, 3), 60, dtype=np.uint8)
    val_img = np.hstack([planta_vis, sep, osm_vis])
    val_path = f"/var/www/urbanlex/static/downloads/validacao_pontos_{municipio.replace(' ','_')}.png"
    cv2.imwrite(val_path, val_img)
    logs.append({'nivel': 'ok', 'msg': '  Validacao gerada — confira as linhas coloridas'})

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
    from shapely.ops import unary_union
    zonas_geo = []
    img_h, img_w = img_planta.shape[:2]

    # Pre-processar: remover texto preto/escuro que pode confundir segmentacao
    # Mascarar pixels muito escuros (texto, bordas)
    gray = cv2.cvtColor(img_planta, cv2.COLOR_BGR2GRAY)
    _, texto_mask = cv2.threshold(gray, 60, 255, cv2.THRESH_BINARY_INV)
    kernel_dilate = np.ones((3,3), np.uint8)
    texto_mask = cv2.dilate(texto_mask, kernel_dilate, iterations=1)

    for zona in legenda:
        nome = zona.get('nome', '')
        cor_hex = zona.get('cor_hex', '#000000')
        try:
            r = int(cor_hex[1:3], 16)
            g = int(cor_hex[3:5], 16)
            b = int(cor_hex[5:7], 16)
        except Exception:
            continue

        # Ignorar cores muito escuras (provavelmente texto/borda)
        if r < 40 and g < 40 and b < 40:
            continue

        target = np.array([b, g, r], dtype=np.uint8)

        # Tolerancia adaptativa: maior para cores claras, menor para cores saturadas
        brightness = (int(r) + int(g) + int(b)) / 3
        tol = 40 if brightness > 180 else 30

        lower = np.clip(target.astype(int) - tol, 0, 255).astype(np.uint8)
        upper = np.clip(target.astype(int) + tol, 0, 255).astype(np.uint8)
        mask = cv2.inRange(img_planta, lower, upper)

        # Remover areas de texto da mascara
        mask = cv2.bitwise_and(mask, cv2.bitwise_not(texto_mask))

        # Morfologia: fechar buracos (causados por hachuras e texto interno)
        kernel_close = np.ones((7,7), np.uint8)
        mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, kernel_close, iterations=3)
        kernel_open = np.ones((5,5), np.uint8)
        mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, kernel_open, iterations=1)

        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        if not contours:
            continue

        # Filtrar por area minima E por proporcao (eliminar formas alongadas = texto)
        min_area = img_w * img_h * 0.00005  # mais permissivo
        contours_ok = []
        for c in contours:
            area = cv2.contourArea(c)
            if area < min_area:
                continue
            # Verificar proporcao do bounding rect (eliminar texto alongado)
            x, y, w, h = cv2.boundingRect(c)
            aspect = max(w, h) / max(min(w, h), 1)
            # Texto tende a ter aspect ratio muito alto E area pequena
            if aspect > 8 and area < img_w * img_h * 0.002:
                continue
            # Solidez: area / area convex hull (texto tem solidez baixa)
            hull = cv2.convexHull(c)
            hull_area = cv2.contourArea(hull)
            solidity = area / max(hull_area, 1)
            if solidity < 0.15 and area < img_w * img_h * 0.001:
                continue
            contours_ok.append(c)

        if not contours_ok:
            continue

        logs.append({'nivel': 'info', 'msg': f'  {nome}: {len(contours_ok)} poligono(s)'})

        for cnt in contours_ok:
            # Simplificar contorno para reduzir vertices
            epsilon = 0.002 * cv2.arcLength(cnt, True)
            cnt_simple = cv2.approxPolyDP(cnt, epsilon, True)
            pts = cnt_simple.reshape(-1, 1, 2).astype(np.float32)
            pts_t = cv2.perspectiveTransform(pts, H)
            pts_ll = [px_to_ll(p[0][0], p[0][1]) for p in pts_t]
            if len(pts_ll) >= 3:
                try:
                    poly = Polygon([(p[1], p[0]) for p in pts_ll])
                    if not poly.is_valid:
                        poly = poly.buffer(0)
                    area_km2 = round(poly.area * 111 * 111, 4)
                except Exception:
                    area_km2 = 0
                zonas_geo.append({
                    'nome': nome,
                    'descricao': zona.get('descricao', ''),
                    'cor_hex': cor_hex,
                    'coordenadas': pts_ll,
                    'area_km2': area_km2
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
