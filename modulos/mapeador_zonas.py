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

        # Estágios 3, 4, 5 — a implementar
        logs.append({'nivel': 'aviso', 'msg': '⚠️ Estágios 3-5 (georreferenciamento, segmentação, KML) ainda em implementação'})
        job['result'] = resultado

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

        # Overpass API para buscar vias
        overpass_url = "https://overpass-api.de/api/interpreter"
        query = f"""
[out:json][timeout:30];
(
  way["highway"~"primary|secondary|tertiary|residential|trunk"]
  ({south},{west},{north},{east});
);
out geom;
"""
        r2 = requests.post(overpass_url, data={'data': query}, timeout=45)
        data = r2.json()
        logs.append({'nivel': 'info', 'msg': f'  🛣️ {len(data.get("elements", []))} vias obtidas'})
        return data

    except Exception as e:
        logs.append({'nivel': 'aviso', 'msg': f'⚠️ Erro OSM: {str(e)[:100]}'})
        return None
