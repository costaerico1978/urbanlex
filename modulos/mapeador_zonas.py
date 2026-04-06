"""
Módulo de mapeamento de zonas urbanísticas.
Pipeline: Gemini Vision (legenda) → OSM (eixos viários) → RANSAC (georreferenciamento) → OpenCV (segmentação) → KML
"""
import os
import tempfile
import json

def mapear_zonas(fpath, fname, municipio, estado, logs, job, tmp):
    """Pipeline principal de mapeamento de zonas."""
    resultado = {
        'legenda_ok': False,
        'osm_ok': False,
        'geo_ok': False,
        'zonas_ok': False,
        'kml_ok': False,
        'zonas': [],
        'kml_url': None
    }

    try:
        # Estágio 1: Extrair legenda via Gemini Vision
        logs.append({'nivel': 'ok', 'msg': f'📄 Arquivo recebido: {fname} ({os.path.getsize(fpath)//1024}KB)'})
        logs.append({'nivel': 'info', 'msg': '🔍 Estágio 1/5: Extraindo legenda via Gemini Vision...'})
        legenda = _extrair_legenda(fpath, fname, municipio, estado, logs, tmp)
        if legenda:
            resultado['legenda_ok'] = True
            resultado['zonas'] = legenda
            logs.append({'nivel': 'ok', 'msg': f'✅ Legenda extraída: {len(legenda)} zonas identificadas'})
            for z in legenda:
                logs.append({'nivel': 'info', 'msg': f'  📍 {z["nome"]} — cor: {z["cor_hex"]}'})
        else:
            logs.append({'nivel': 'aviso', 'msg': '⚠️ Não foi possível extrair a legenda'})
            job['result'] = resultado
            return

        # Estágio 2: Buscar eixos viários no OSM
        logs.append({'nivel': 'info', 'msg': f'🌐 Estágio 2/5: Buscando eixos viários de {municipio}/{estado} no OpenStreetMap...'})
        osm_data = _buscar_osm(municipio, estado, logs)
        if osm_data:
            resultado['osm_ok'] = True
            logs.append({'nivel': 'ok', 'msg': f'✅ OSM: {len(osm_data.get("elements", []))} elementos viários encontrados'})
        else:
            logs.append({'nivel': 'aviso', 'msg': '⚠️ Não foi possível obter dados OSM'})
            job['result'] = resultado
            return

        # Estagio 3: Georreferenciamento
        logs.append({"nivel": "info", "msg": "📐 Estágio 3/5: Georreferenciando planta com eixos OSM..."})
        import cv2, numpy as np
        # Carregar imagem da planta
        ext = os.path.splitext(fname)[1].lower()
        if ext == ".pdf":
            pages = sorted([x for x in os.listdir(tmp) if x.startswith("mapa_") and x.endswith(".png")])
            img_path = os.path.join(tmp, pages[0]) if pages else None
        else:
            img_path = fpath
        if not img_path or not os.path.exists(img_path):
            logs.append({"nivel": "aviso", "msg": "⚠️ Imagem da planta não encontrada"})
            job["result"] = resultado
            return
        img_planta = cv2.imread(img_path)
        img_h, img_w = img_planta.shape[:2]
        # Redimensionar para max 2000px para evitar OOM
        _MAX_W = 2000
        if img_w > _MAX_W:
            _scale = _MAX_W / img_w
            img_planta = cv2.resize(img_planta, (int(img_w * _scale), int(img_h * _scale)), interpolation=cv2.INTER_AREA)
            img_h, img_w = img_planta.shape[:2]
            logs.append({"nivel": "info", "msg": f"  📐 Imagem redimensionada para {img_w}x{img_h}px"})
        logs.append({"nivel": "info", "msg": f"  📏 Dimensões da planta: {img_w}x{img_h}px"})
        # Extrair vias da planta e renderizar OSM
        img_vias_planta = _extrair_vias_planta(img_planta)
        bbox_tuple = (float(osm_data.get("_south", -29.86)), float(osm_data.get("_north", -29.76)),
                      float(osm_data.get("_west", -50.12)), float(osm_data.get("_east", -50.02)))
        img_vias_osm = _renderizar_osm(osm_data, bbox_tuple, img_w, img_h)
        # Salvar figura-fundos para visualizacao
        import cv2 as _cv2_ff
        _ff_planta_path = f"/var/www/urbanlex/static/downloads/ff_planta_{municipio.replace(chr(32),chr(95))}.png"
        _ff_osm_path = f"/var/www/urbanlex/static/downloads/ff_osm_{municipio.replace(chr(32),chr(95))}.png"
        _cv2_ff.imwrite(_ff_planta_path, img_vias_planta)
        _cv2_ff.imwrite(_ff_osm_path, img_vias_osm)
        resultado["ff_planta_url"] = f"/static/downloads/ff_planta_{municipio.replace(chr(32),chr(95))}.png"
        resultado["ff_osm_url"] = f"/static/downloads/ff_osm_{municipio.replace(chr(32),chr(95))}.png"
        logs.append({"nivel": "ok", "msg": "🖼️ Figura-fundos salvos — verifique antes de continuar"})
        job["result"] = resultado
        geo_result = _georreferenciar(img_vias_planta, img_vias_osm, bbox_tuple, img_w, img_h, logs)
        if not geo_result:
            logs.append({"nivel": "aviso", "msg": "⚠️ Georreferenciamento falhou — verifique a qualidade da planta"})
            job["result"] = resultado
            return
        H, px_to_ll = geo_result
        resultado["geo_ok"] = True
        logs.append({"nivel": "ok", "msg": "✅ Georreferenciamento concluído"})
        # Loop de refinamento guiado por Gemini Vision
        import cv2 as _cv2_val
        import numpy as _np_val
        import base64 as _b64
        from google import genai as _gv_ref
        from google.genai import types as _gv_ref_types
        import json as _jref, re as _rref
        _client_ref = _gv_ref.Client(api_key=os.environ.get("GEMINI_API_KEY",""))
        _M_atual = H.copy()
        _val_path = f"/var/www/urbanlex/static/downloads/validacao_{municipio.replace(chr(32),chr(95))}.png"

        def _gerar_validacao(M):
            _val = _np_val.zeros((img_h, img_w, 3), dtype=_np_val.uint8)
            _pw = _cv2_val.warpAffine(img_vias_planta, M[:2,:], (img_w, img_h))
            _val[:,:,0] = _pw       # azul = planta
            _val[:,:,2] = img_vias_osm  # vermelho = OSM
            _cv2_val.imwrite(_val_path, _val)
            return _val

        _val_img = _gerar_validacao(_M_atual)
        logs.append({"nivel": "ok", "msg": "🖼️ Validação inicial gerada"})

        for _iter in range(5):
            logs.append({"nivel": "info", "msg": f"🤖 Gemini analisando alinhamento (iteração {_iter+1}/5)..."})
            with open(_val_path, "rb") as _vf:
                _vbytes = _vf.read()
            _vprompt = (
                "Esta imagem mostra a sobreposição de duas redes viárias urbanas.\n"
                "AZUL = planta de zoneamento (deve ser ajustada).\n"
                "VERMELHO = eixos viários OSM (referência fixa, não muda).\n"
                "ROXO = onde as duas redes coincidem (desejado).\n\n"
                "Analise o alinhamento e responda APENAS com JSON:\n"
                "{\"alinhado\": true/false, \"dx\": pixels_horizontal, \"dy\": pixels_vertical, \"rotacao\": graus, \"escala\": fator, \"descricao\": \"texto\"}\n"
                "dx positivo = mover direita, dy positivo = mover baixo.\n"
                "Se alinhado=true, retorne zeros nos ajustes."
            )
            try:
                _parts = [
                    _gv_ref_types.Part.from_text(text=_vprompt),
                    _gv_ref_types.Part.from_bytes(data=_vbytes, mime_type="image/png")
                ]
                import concurrent.futures as _cff
                _exx = _cff.ThreadPoolExecutor(max_workers=1)
                _futt = _exx.submit(_client_ref.models.generate_content, model="gemini-2.5-flash", contents=_parts)
                try:
                    _resp_ref = _futt.result(timeout=60)
                    _exx.shutdown(wait=False)
                except _cff.TimeoutError:
                    _exx.shutdown(wait=False)
                    logs.append({"nivel": "aviso", "msg": "  Gemini timeout — parando refinamento"})
                    break
                _resp_txt = _rref.sub(r"^```json\s*|\s*```$", "", _resp_ref.text.strip())
                _adj = _jref.loads(_resp_txt)
                logs.append({"nivel": "info", "msg": f"  Gemini: {_adj.get(chr(100)+chr(101)+chr(115)+chr(99)+chr(114)+chr(105)+chr(231)+chr(227)+chr(111), chr(115)+chr(101)+chr(109))}"})
                if _adj.get("alinhado"):
                    logs.append({"nivel": "ok", "msg": "  ✅ Gemini confirmou alinhamento!"})
                    break
                _dx = float(_adj.get("dx", 0))
                _dy = float(_adj.get("dy", 0))
                _rot = float(_adj.get("rotacao", 0))
                _esc = float(_adj.get("escala", 1.0))
                if abs(_dx) < 2 and abs(_dy) < 2 and abs(_rot) < 0.5 and abs(_esc-1) < 0.01:
                    logs.append({"nivel": "ok", "msg": "  ✅ Ajustes mínimos — alinhamento aceito"})
                    break
                # Aplicar ajuste incremental
                _cx, _cy = img_w/2.0, img_h/2.0
                _rad = _np_val.radians(_rot)
                _R_adj = _np_val.array([
                    [_esc*_np_val.cos(_rad), -_esc*_np_val.sin(_rad), _dx],
                    [_esc*_np_val.sin(_rad),  _esc*_np_val.cos(_rad), _dy]
                ], dtype=_np_val.float32)
                _R_adj3 = _np_val.eye(3, dtype=_np_val.float32)
                _R_adj3[:2,:] = _R_adj
                _M_atual = _R_adj3 @ _M_atual
                _val_img = _gerar_validacao(_M_atual)
                logs.append({"nivel": "info", "msg": f"  Ajuste aplicado: dx={_dx:.1f} dy={_dy:.1f} rot={_rot:.1f}° esc={_esc:.3f}"})
            except Exception as _er:
                logs.append({"nivel": "aviso", "msg": f"  Erro Gemini: {str(_er)[:80]}"})
                break

        H = _M_atual
        resultado["validacao_url"] = f"/static/downloads/validacao_{municipio.replace(chr(32),chr(95))}.png"
        logs.append({"nivel": "ok", "msg": "🖼️ Imagem de validação final disponível"})
        logs.append({"nivel": "info", "msg": "🎨 Estágio 4/5: Segmentando zonas por cor..."})
        zonas_geo = _segmentar_zonas(img_planta, legenda, H, px_to_ll, logs)
        if zonas_geo:
            resultado["zonas_ok"] = True
            resultado["zonas"] = [{"nome": z["nome"], "descricao": z.get("descricao",""), "cor": z["cor_hex"], "area_km2": z.get("area_km2","—")} for z in zonas_geo]
            logs.append({"nivel": "ok", "msg": f"✅ {len(zonas_geo)} polígonos segmentados"})
        # Estagio 5: KML
        logs.append({"nivel": "info", "msg": "📦 Estágio 5/5: Gerando KML..."})
        kml_path = os.path.join("/var/www/urbanlex/static/downloads", f"zoneamento_{municipio.replace(chr(32),chr(95))}.kml")
        _gerar_kml(zonas_geo, municipio, estado, kml_path)
        resultado["kml_ok"] = True
        resultado["kml_url"] = f"/static/downloads/zoneamento_{municipio.replace(chr(32),chr(95))}.kml"
        logs.append({"nivel": "ok", "msg": "✅ KML gerado com sucesso!"})
        job["result"] = resultado

    except Exception as e:
        logs.append({'nivel': 'erro', 'msg': f'Erro: {str(e)[:200]}'})
        job['result'] = resultado


def _extrair_legenda(fpath, fname, municipio, estado, logs, tmp):
    """Extrai zonas e cores da legenda via Gemini Vision."""
    import os
    from google import genai as _gv
    from google.genai import types as _gv_types

    try:
        client = _gv.Client(api_key=os.environ.get('GEMINI_API_KEY', ''))
        ext = os.path.splitext(fname)[1].lower()

        # Converter PDF para imagem se necessário
        if ext == '.pdf':
            import subprocess
            subprocess.run([
                'gs', '-dNOPAUSE', '-dBATCH', '-sDEVICE=png16m', '-r150',
                f'-sOutputFile={tmp}/mapa_%03d.png', fpath
            ], capture_output=True, timeout=120)
            pages = sorted([x for x in os.listdir(tmp) if x.startswith('mapa_') and x.endswith('.png')])
            if not pages:
                return None
            img_path = os.path.join(tmp, pages[0])  # primeira página
        else:
            img_path = fpath

        with open(img_path, 'rb') as fp:
            img_bytes = fp.read()

        import mimetypes
        mime = mimetypes.guess_type(img_path)[0] or 'image/png'

        prompt = (
            f"Esta é uma planta de zoneamento municipal de {municipio}/{estado}.\n"
            f"Analise a LEGENDA do mapa e liste TODAS as zonas/subzonas presentes.\n"
            f"Para cada zona, identifique:\n"
            f"1. O nome/código da zona (ex: ZR1, ZC2, ZEIS)\n"
            f"2. A descrição completa (ex: Zona Residencial 1)\n"
            f"3. A cor predominante em formato HEX (ex: #FFD700)\n\n"
            f"Responda APENAS com JSON:\n"
            f'[{{"nome":"ZR1","descricao":"Zona Residencial 1","cor_hex":"#FFD700"}}]'
        )

        parts = [
            _gv_types.Part.from_text(text=prompt),
            _gv_types.Part.from_bytes(data=img_bytes, mime_type=mime)
        ]

        import concurrent.futures as _cf
        ex = _cf.ThreadPoolExecutor(max_workers=1)
        fut = ex.submit(client.models.generate_content, model='gemini-2.5-flash', contents=parts)
        try:
            resp = fut.result(timeout=120)
            ex.shutdown(wait=False)
        except _cf.TimeoutError:
            ex.shutdown(wait=False)
            logs.append({'nivel': 'aviso', 'msg': '⚠️ Gemini Vision timeout na extração da legenda'})
            return None

        if not resp or not resp.text:
            return None

        import re, json as _j
        txt = re.sub(r'^```json\s*|\s*```$', '', resp.text.strip())
        return _j.loads(txt)

    except Exception as e:
        logs.append({'nivel': 'aviso', 'msg': f'⚠️ Erro ao extrair legenda: {str(e)[:100]}'})
        return None


def _buscar_osm(municipio, estado, logs):
    """Busca eixos viários do município via Overpass API (OSM)."""
    import requests

    # Primeiro buscar o bbox do município
    try:
        # Nominatim para obter bbox
        nominatim_url = f"https://nominatim.openstreetmap.org/search"
        params = {
            'q': f'{municipio}, {estado}, Brasil',
            'format': 'json',
            'limit': 1,
            'addressdetails': 1
        }
        headers = {'User-Agent': 'UrbanLex/1.0'}
        r = requests.get(nominatim_url, params=params, headers=headers, timeout=15)
        results = r.json()
        if not results:
            logs.append({'nivel': 'aviso', 'msg': f'⚠️ Município {municipio}/{estado} não encontrado no OSM'})
            return None

        bbox = results[0].get('boundingbox', [])
        if len(bbox) < 4:
            return None

        south, north, west, east = bbox[0], bbox[1], bbox[2], bbox[3]
        logs.append({'nivel': 'info', 'msg': f'  📌 Bbox: {south},{west} → {north},{east}'})

        # Overpass API para buscar vias — tenta multiplos servidores
        _servers = [
            "https://overpass-api.de/api/interpreter",
            "https://overpass.kumi.systems/api/interpreter",
            "https://overpass.openstreetmap.fr/api/interpreter",
        ]
        query = "[out:json][timeout:30];(way[\"highway\"~\"primary|secondary|tertiary|residential|trunk\"]"
        query += f"({south},{west},{north},{east}););out geom;"
        data = None
        import time as _t
        for _srv in _servers:
            try:
                logs.append({"nivel": "info", "msg": f"  Tentando: {_srv.split(chr(47))[2]}..."})
                _t.sleep(2)
                r2 = requests.post(_srv, data={"data": query}, timeout=60)
                if r2.status_code == 200 and r2.text.strip().startswith("{"):
                    data = r2.json()
                    if data.get("elements"):
                        break
                    logs.append({"nivel": "aviso", "msg": f"  0 elementos em {_srv.split(chr(47))[2]}"})
                else:
                    logs.append({"nivel": "aviso", "msg": f"  Servidor indisponivel: {_srv.split(chr(47))[2]}"})
            except Exception as _se:
                logs.append({"nivel": "aviso", "msg": f"  Erro {_srv.split(chr(47))[2]}: {str(_se)[:60]}"})
        if not data or not data.get("elements"):
            return None
        logs.append({"nivel": "info", "msg": f"  {len(data.get(chr(101)+chr(108)+chr(101)+chr(109)+chr(101)+chr(110)+chr(116)+chr(115), []))} vias obtidas"})
        data["_south"] = float(south)
        data["_north"] = float(north)
        data["_west"] = float(west)
        data["_east"] = float(east)
        return data

    except Exception as e:
        logs.append({'nivel': 'aviso', 'msg': f'⚠️ Erro OSM: {str(e)[:100]}'})
        return None


def _renderizar_osm(osm_data, bbox, img_w, img_h):
    """Renderiza eixos viários OSM como imagem numpy."""
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
                cv2.line(img, pts[i], pts[i+1], 255, 2)

    return img


def _extrair_vias_planta(img_planta):
    """Extrai bordas entre zonas coloridas via Canny — sem skeleton."""
    import numpy as np
    import cv2
    lab = cv2.cvtColor(img_planta, cv2.COLOR_BGR2LAB)
    bordas = np.zeros(img_planta.shape[:2], dtype=np.uint8)
    for ch in cv2.split(lab):
        blur = cv2.GaussianBlur(ch, (3, 3), 0)
        b = cv2.Canny(blur, 30, 90)
        bordas = cv2.bitwise_or(bordas, b)
    return bordas


def _georreferenciar(img_vias_planta, img_vias_osm, bbox, img_w, img_h, logs):
    """Alinha planta ao OSM via transformacao afim (translacao+rotacao+escala) + RANSAC."""
    import numpy as np
    import cv2
    south, north, west, east = bbox

    def _extrair_segmentos(img):
        linhas = cv2.HoughLinesP(img, rho=1, theta=np.pi/180,
                                  threshold=20, minLineLength=15, maxLineGap=5)
        if linhas is None:
            return None
        return linhas.reshape(-1, 4)

    def _pontos_medios(segs):
        return np.array([[(s[0]+s[2])/2.0, (s[1]+s[3])/2.0] for s in segs], dtype=np.float32)

    segs_planta = _extrair_segmentos(img_vias_planta)
    segs_osm = _extrair_segmentos(img_vias_osm)

    if segs_planta is None or segs_osm is None:
        logs.append({"nivel": "aviso", "msg": f"  Segmentos insuficientes: planta={len(segs_planta) if segs_planta is not None else 0}, osm={len(segs_osm) if segs_osm is not None else 0}"})
        return None

    logs.append({"nivel": "info", "msg": f"  Segmentos: planta={len(segs_planta)}, osm={len(segs_osm)}"})

    pts_planta = _pontos_medios(segs_planta)
    pts_osm = _pontos_medios(segs_osm)

    MAX_PTS = 500
    if len(pts_planta) > MAX_PTS:
        idx = np.random.choice(len(pts_planta), MAX_PTS, replace=False)
        pts_planta = pts_planta[idx]
    if len(pts_osm) > MAX_PTS:
        idx = np.random.choice(len(pts_osm), MAX_PTS, replace=False)
        pts_osm = pts_osm[idx]

    best_M = None
    best_inliers = 0
    cx, cy = img_w / 2.0, img_h / 2.0

    for angle in [0, 45, 90, 135, 180, 225, 270, 315]:
        rad = np.radians(angle)
        R = np.array([[np.cos(rad), -np.sin(rad)],
                       [np.sin(rad),  np.cos(rad)]], dtype=np.float32)
        pts_rot = (R @ (pts_planta - [cx, cy]).T).T + [cx, cy]
        n = min(len(pts_rot), len(pts_osm), 200)
        idx_p = np.random.choice(len(pts_rot), n, replace=False)
        idx_o = np.random.choice(len(pts_osm), n, replace=False)
        M, mask = cv2.estimateAffinePartial2D(
            pts_rot[idx_p], pts_osm[idx_o],
            method=cv2.RANSAC, ransacReprojThreshold=15.0)
        if M is not None and mask is not None:
            inliers = int(mask.sum())
            logs.append({"nivel": "info", "msg": f"  Rotacao {angle}°: {inliers} inliers"})
            if inliers > best_inliers:
                best_inliers = inliers
                # Combinar pre-rotacao com M afim
                R_full = np.eye(3, dtype=np.float32)
                R_full[:2, :2] = R
                R_full[:2, 2] = [cx - R[0,0]*cx - R[0,1]*cy,
                                  cy - R[1,0]*cx - R[1,1]*cy]
                M_full = np.eye(3, dtype=np.float32)
                M_full[:2, :] = M
                best_M = M_full @ R_full

    if best_M is None or best_inliers < 5:
        logs.append({"nivel": "aviso", "msg": f"  Georreferenciamento insuficiente: {best_inliers} inliers"})
        return None

    logs.append({"nivel": "ok", "msg": f"  Melhor alinhamento: {best_inliers} inliers"})

    def px_to_ll(x, y):
        lon = west + (x / img_w) * (east - west)
        lat = north - (y / img_h) * (north - south)
        return lat, lon

    return best_M, px_to_ll


def _segmentar_zonas(img_planta, legenda, H, px_to_ll, logs):
    """Segmenta zonas por cor e converte para polígonos georreferenciados."""
    import numpy as np
    import cv2
    from shapely.geometry import Polygon
    import json

    zonas_geo = []
    img_h, img_w = img_planta.shape[:2]

    for zona in legenda:
        nome = zona.get('nome', '')
        cor_hex = zona.get('cor_hex', '#000000')

        # Converter hex para BGR
        try:
            r = int(cor_hex[1:3], 16)
            g = int(cor_hex[3:5], 16)
            b = int(cor_hex[5:7], 16)
        except Exception:
            continue

        # Criar máscara de cor com tolerância
        target = np.array([b, g, r], dtype=np.uint8)
        lower = np.clip(target.astype(int) - 25, 0, 255).astype(np.uint8)
        upper = np.clip(target.astype(int) + 25, 0, 255).astype(np.uint8)
        mask = cv2.inRange(img_planta, lower, upper)

        # Encontrar contornos
        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        if not contours:
            continue

        # Filtrar contornos pequenos
        min_area = img_w * img_h * 0.0001
        contours = [c for c in contours if cv2.contourArea(c) > min_area]
        if not contours:
            continue

        logs.append({'nivel': 'info', 'msg': f'  🎨 {nome}: {len(contours)} polígono(s) encontrado(s)'})

        for cnt in contours:
            # Aplicar homografia nos pontos do contorno
            pts = cnt.reshape(-1, 1, 2).astype(np.float32)
            pts_transformed = cv2.perspectiveTransform(pts, H)
            pts_ll = [px_to_ll(p[0][0], p[0][1]) for p in pts_transformed]

            if len(pts_ll) >= 3:
                zonas_geo.append({
                    'nome': nome,
                    'descricao': zona.get('descricao', ''),
                    'cor_hex': cor_hex,
                    'coordenadas': pts_ll,
                    'area_km2': round(Polygon([(p[1], p[0]) for p in pts_ll]).area * 111 * 111, 4)
                })

    logs.append({'nivel': 'ok', 'msg': f'  ✅ {len(zonas_geo)} polígonos georreferenciados'})
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

        # Cor
        try:
            hex_c = zona['cor_hex'].lstrip('#')
            r, g, b = int(hex_c[0:2], 16), int(hex_c[2:4], 16), int(hex_c[4:6], 16)
            pol.style.polystyle.color = simplekml.Color.rgb(r, g, b, a=180)
            pol.style.linestyle.color = simplekml.Color.rgb(r, g, b)
            pol.style.linestyle.width = 1
        except Exception:
            pass

        pol.description = f'{zona.get("descricao", "")} — {zona.get("area_km2", "?")} km²'

    kml.save(output_path)
    return output_path
