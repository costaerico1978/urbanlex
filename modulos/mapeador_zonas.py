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
        H, px_to_ll = geo_result
        resultado['geo_ok'] = True

        # Gerar validacao
        import numpy as _np
        img_vias_osm = _renderizar_osm(osm_data, bbox_tuple, img_w, img_h)
        _val = _np.zeros((img_h, img_w, 3), dtype=_np.uint8)
        _pw = cv2.warpAffine(img_planta, H[:2, :], (img_w, img_h))
        _pw_gray = cv2.cvtColor(_pw, cv2.COLOR_BGR2GRAY)
        _, _pw_bin = cv2.threshold(_pw_gray, 30, 255, cv2.THRESH_BINARY)
        _val[:, :, 0] = _pw_bin
        _val[:, :, 2] = img_vias_osm
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


def _georreferenciar_gemini(img_planta, osm_data, bbox, img_w, img_h, municipio, estado, logs, tmp):
    """
    Georreferencia a planta usando Gemini Vision para identificar vias nomeadas
    e geocodifica-las via OSM Nominatim para obter coordenadas reais.
    """
    import numpy as np
    import cv2
    import requests
    import re
    import json as _j
    import concurrent.futures as _cf
    import time
    from google import genai as _gv
    from google.genai import types as _gv_types

    south, north, west, east = bbox

    # Salvar imagem para Gemini
    _img_path = f"{tmp}/planta_geo.png"
    cv2.imwrite(_img_path, img_planta)

    logs.append({'nivel': 'info', 'msg': '  Gemini identificando vias nomeadas na planta...'})
    try:
        client = _gv.Client(api_key=os.environ.get('GEMINI_API_KEY', ''))
        with open(_img_path, 'rb') as fp:
            img_bytes = fp.read()

        prompt = (
            f"Esta e uma planta de zoneamento de {municipio}/{estado}, Brasil.\n"
            f"Identifique todas as vias/ruas/avenidas/estradas com NOME VISIVEL no mapa.\n"
            f"Para cada via nomeada, informe:\n"
            f"1. O nome exato da via\n"
            f"2. A posicao X do centro da via como porcentagem da largura da imagem (0-100)\n"
            f"3. A posicao Y do centro da via como porcentagem da altura da imagem (0-100)\n\n"
            f"Responda APENAS com JSON valido, sem texto adicional:\n"
            f'[{{"nome":"Estrada do Mar","x_pct":45.5,"y_pct":72.3}}]\n\n'
            f"Identifique ao menos 4 vias diferentes se possivel."
        )

        parts = [_gv_types.Part.from_text(text=prompt),
                 _gv_types.Part.from_bytes(data=img_bytes, mime_type='image/png')]

        ex = _cf.ThreadPoolExecutor(max_workers=1)
        fut = ex.submit(client.models.generate_content, model='gemini-2.5-flash', contents=parts)
        try:
            resp = fut.result(timeout=120)
            ex.shutdown(wait=False)
        except _cf.TimeoutError:
            ex.shutdown(wait=False)
            logs.append({'nivel': 'aviso', 'msg': '  Gemini timeout na identificacao de vias'})
            return None

        txt = re.sub(r'^```json\s*|\s*```$', '', resp.text.strip())
        vias_planta = _j.loads(txt)
        logs.append({'nivel': 'info', 'msg': f'  {len(vias_planta)} vias identificadas pelo Gemini'})
        for v in vias_planta:
            logs.append({'nivel': 'info', 'msg': f'    - {v["nome"]} ({v["x_pct"]:.1f}%, {v["y_pct"]:.1f}%)'})

    except Exception as e:
        logs.append({'nivel': 'aviso', 'msg': f'  Erro Gemini: {str(e)[:100]}'})
        return None

    # Geocodificar cada via via OSM Nominatim
    logs.append({'nivel': 'info', 'msg': '  Geocodificando vias via OSM Nominatim...'})
    correspondencias = []
    for via in vias_planta:
        nome = via.get('nome', '')
        if not nome:
            continue
        try:
            r = requests.get("https://nominatim.openstreetmap.org/search",
                             params={"q": f"{nome}, {municipio}, {estado}, Brasil",
                                     "format": "json", "limit": 1,
                                     "viewbox": f"{west},{north},{east},{south}",
                                     "bounded": 1},
                             headers={"User-Agent": "UrbanLex/1.0"}, timeout=10)
            results = r.json()
            if results:
                lat = float(results[0]['lat'])
                lon = float(results[0]['lon'])
                px = (via['x_pct'] / 100.0) * img_w
                py = (via['y_pct'] / 100.0) * img_h
                correspondencias.append({'nome': nome, 'px': px, 'py': py, 'lat': lat, 'lon': lon})
                logs.append({'nivel': 'info', 'msg': f'    OK {nome}: ({lat:.4f}, {lon:.4f})'})
            else:
                logs.append({'nivel': 'aviso', 'msg': f'    Nao geocodificado: {nome}'})
        except Exception as e:
            logs.append({'nivel': 'aviso', 'msg': f'    Erro {nome}: {str(e)[:50]}'})
        time.sleep(1)

    if len(correspondencias) < 3:
        logs.append({'nivel': 'aviso', 'msg': f'  Apenas {len(correspondencias)} correspondencias — insuficiente (minimo 3)'})
        return None

    logs.append({'nivel': 'ok', 'msg': f'  {len(correspondencias)} correspondencias para georreferenciamento'})

    def ll_to_osm_px(lat, lon):
        x = (lon - west) / (east - west) * img_w
        y = (north - lat) / (north - south) * img_h
        return x, y

    src_pts = np.array([[c['px'], c['py']] for c in correspondencias], dtype=np.float32)
    dst_pts = np.array([ll_to_osm_px(c['lat'], c['lon']) for c in correspondencias], dtype=np.float32)

    M, inliers = cv2.estimateAffinePartial2D(src_pts, dst_pts, method=cv2.RANSAC, ransacReprojThreshold=20.0)
    if M is None:
        logs.append({'nivel': 'aviso', 'msg': '  Falha ao calcular transformacao afim'})
        return None
    inliers_count = int(inliers.sum()) if inliers is not None else 0
    logs.append({'nivel': 'ok', 'msg': f'  Transformacao afim calculada: {inliers_count} inliers'})

    M_full = np.eye(3, dtype=np.float32)
    M_full[:2, :] = M

    def px_to_ll(x, y):
        lon = west + (x / img_w) * (east - west)
        lat = north - (y / img_h) * (north - south)
        return lat, lon

    return M_full, px_to_ll


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
