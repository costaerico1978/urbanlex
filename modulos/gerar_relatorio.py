"""
Gerador de relatório PDF de legislação urbanística — UrbanLex
Usa WeasyPrint (HTML→PDF) com fallback para ReportLab
"""
import os, json, datetime

CORES_CAT = {
    'plano_diretor': '#7c6af7',
    'zoneamento':    '#1D9E75',
    'uso_ocupacao':  '#1D9E75',
    'parcelamento':  '#378ADD',
    'codigo_obras':  '#BA7517',
    'outros':        '#888780',
}
LABELS_CAT = {
    'plano_diretor': 'Plano Diretor',
    'zoneamento':    'Zoneamento',
    'uso_ocupacao':  'Uso e Ocupação',
    'parcelamento':  'Parcelamento do Solo',
    'codigo_obras':  'Código de Obras',
    'outros':        'Outros',
}

def _esc(s):
    if not s:
        return ''
    return str(s).replace('&','&amp;').replace('<','&lt;').replace('>','&gt;').replace('"','&quot;')

def _fmt_lista(lst):
    if not lst:
        return '—'
    if isinstance(lst[0], dict):
        parts = []
        for o in lst:
            lei = _esc(o.get('lei',''))
            partes = _esc(o.get('partes',''))
            if partes:
                parts.append(f'{lei}<br><span style="font-size:9px;color:#888;margin-left:10px">└ {partes}</span>')
            else:
                parts.append(lei)
        return '<br>'.join(parts)
    return '<br>'.join(_esc(str(x)) for x in lst if x)

def _anexo_badge(nome_lower):
    if any(p in nome_lower for p in ['tabela','parâmetro','parametro','índice','indice','ca ','to ','ga ','coeficiente']):
        return '<span class="badge-tabela">tabela de parâmetros</span>'
    if any(p in nome_lower for p in ['mapa','zona','subzona','zoneamento','planta','anexo xxi','anexo 21']):
        return '<span class="badge-mapa">mapa de zonas</span>'
    return '<span class="badge-doc">doc</span>'

def gerar_html_relatorio(resultado, municipio, estado, custo_usd=None, token_stats=None, tempo_segundos=None, nao_encontradas=None):
    enc = resultado.get('encontradas', [])
    legs_json = resultado.get('legislacoes_json', [])
    agora = datetime.datetime.now().strftime('%d/%m/%Y às %H:%M')
    custo_str = f'US$ {custo_usd:.4f} (≈ R$ {custo_usd*5.8:.2f})' if custo_usd else '—'
    tokens_str = ''
    if token_stats:
        tokens_str = f"{token_stats.get('input',0):,} entrada · {token_stats.get('output',0):,} saída"
    tempo_str = ''
    if tempo_segundos:
        m, s = divmod(int(tempo_segundos), 60)
        tempo_str = f'{m} min {s} seg'

    # Contar categorias
    cats_enc = set(l.get('categoria','outros') for l in legs_json)
    nao_loc = nao_encontradas or []

    # Detectar plantas/mapas e tabelas
    plantas_encontradas = []
    plantas_nao_encontradas = []
    for leg in enc:
        for anx in (leg.get('anexos_lm') or []):
            nome = (anx.get('nome') or '').lower()
            if any(p in nome for p in ['mapa','zona','subzona','zoneamento','planta']):
                plantas_encontradas.append({'lei': f"{leg.get('tipo','')} {leg.get('numero','')}/{leg.get('ano','')}", 'anx': anx.get('nome','')})
            if any(p in nome for p in ['tabela','parâmetro','parametro','índice','indice']):
                plantas_encontradas.append({'lei': f"{leg.get('tipo','')} {leg.get('numero','')}/{leg.get('ano','')}", 'anx': anx.get('nome',''), 'tipo': 'tabela'})
    for n in nao_loc:
        desc = (n.get('descricao') or '').lower()
        if any(p in desc for p in ['mapa','zona','subzona','zoneamento','planta','decreto','zoneamento ambiental']):
            plantas_nao_encontradas.append(n)

    # ── HTML ──────────────────────────────────────────────────────
    html = f'''<!DOCTYPE html>
<html lang="pt-BR"><head>
<meta charset="UTF-8">
<title>Relatório Urbanístico — {_esc(municipio)} {_esc(estado)}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:'IBM Plex Sans',sans-serif;background:#f5f5f0;color:#1a1a1a;font-size:11px;line-height:1.5}}
  .page{{background:#fff;width:210mm;min-height:297mm;margin:0 auto;padding:0}}
  @media print{{body{{background:#fff}}.page{{margin:0;box-shadow:none}}}}
  /* CAPA */
  .capa-bar{{background:#0f0f1a;padding:18px 28px;display:flex;justify-content:space-between;align-items:center}}
  .capa-logo{{font-family:'IBM Plex Mono',monospace;color:#7c6af7;font-size:14px;letter-spacing:3px;font-weight:500}}
  .capa-tag{{font-size:9px;background:rgba(124,106,247,0.2);color:#7c6af7;border:0.5px solid #7c6af7;border-radius:3px;padding:2px 8px;font-family:'IBM Plex Mono',monospace}}
  .capa-title{{padding:20px 28px 16px;border-bottom:1px solid #e8e8e0}}
  .capa-mun{{font-size:22px;font-weight:600;color:#0f0f1a;margin-bottom:3px}}
  .capa-sub{{font-size:11px;color:#666;margin-bottom:10px}}
  .capa-meta{{display:flex;gap:18px;flex-wrap:wrap}}
  .meta-item{{font-size:9px;color:#999;font-family:'IBM Plex Mono',monospace}}
  .meta-item strong{{color:#444}}
  /* SEÇÕES */
  .sec-divider{{background:#f0f0e8;padding:5px 28px;font-size:9px;color:#888;font-family:'IBM Plex Mono',monospace;letter-spacing:1px;border-bottom:1px solid #e8e8e0;border-top:1px solid #e8e8e0;margin-top:16px}}
  .sec{{padding:14px 28px}}
  .sec-label{{font-size:8px;font-weight:600;letter-spacing:1.5px;color:#aaa;text-transform:uppercase;font-family:'IBM Plex Mono',monospace;margin-bottom:10px}}
  /* CARDS */
  .cards{{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:12px}}
  .card{{background:#f8f8f4;border-radius:6px;padding:10px 12px;border:0.5px solid #e0e0d8}}
  .card-num{{font-size:24px;font-weight:600;font-family:'IBM Plex Mono',monospace}}
  .card-lbl{{font-size:9px;color:#888;margin-top:2px}}
  .card-num.ok{{color:#1D9E75}}.card-num.warn{{color:#BA7517}}.card-num.err{{color:#E24B4A}}
  /* ALERTAS */
  .alert{{display:flex;gap:8px;border-radius:5px;padding:7px 10px;margin-bottom:5px;font-size:10px;line-height:1.4}}
  .alert-icon{{font-size:11px;flex-shrink:0;margin-top:1px}}
  .alert.danger{{background:rgba(226,75,74,0.06);border:0.5px solid rgba(226,75,74,0.25)}}
  .alert.warn{{background:rgba(186,117,23,0.06);border:0.5px solid rgba(186,117,23,0.25)}}
  .alert.ok{{background:rgba(29,158,117,0.06);border:0.5px solid rgba(29,158,117,0.3)}}
  /* PLANTAS */
  .planta-row{{display:flex;align-items:flex-start;gap:10px;background:#f8f8f4;border-radius:5px;padding:9px 12px;margin-bottom:6px;border:0.5px solid #e0e0d8}}
  .planta-row.nok{{border-color:rgba(226,75,74,0.25)}}
  .planta-icon{{width:30px;height:30px;border-radius:5px;display:flex;align-items:center;justify-content:center;font-size:14px;flex-shrink:0}}
  .planta-icon.found{{background:rgba(29,158,117,0.1)}}.planta-icon.miss{{background:rgba(226,75,74,0.07)}}
  .planta-title{{font-size:11px;font-weight:500;color:#1a1a1a;margin-bottom:2px}}
  .planta-desc{{font-size:9px;color:#777}}
  /* PILLS */
  .pill{{display:inline-block;font-size:9px;padding:1px 6px;border-radius:3px;font-family:'IBM Plex Mono',monospace;margin-left:6px;vertical-align:middle}}
  .pill.ok{{background:rgba(29,158,117,0.1);color:#1D9E75;border:0.5px solid rgba(29,158,117,0.3)}}
  .pill.nok{{background:rgba(226,75,74,0.08);color:#E24B4A;border:0.5px solid rgba(226,75,74,0.25)}}
  .pill.warn{{background:rgba(186,117,23,0.08);color:#BA7517;border:0.5px solid rgba(186,117,23,0.25)}}
  /* LEGISLAÇÕES */
  .leg-row{{display:flex;margin-bottom:10px;background:#f8f8f4;border-radius:6px;overflow:hidden;border:0.5px solid #e0e0d8;page-break-inside:avoid}}
  .leg-strip{{width:4px;flex-shrink:0}}
  .leg-body{{padding:10px 12px;flex:1}}
  .leg-title{{font-size:12px;font-weight:600;color:#0f0f1a;margin-bottom:3px}}
  .leg-desc{{font-size:9px;color:#666;margin-bottom:8px;line-height:1.5}}
  .anx-list{{margin-top:4px}}
  .anx-row{{display:flex;align-items:flex-start;gap:6px;font-size:9px;color:#555;margin-bottom:3px}}
  .badge-tabela{{font-size:8px;padding:1px 5px;border-radius:2px;background:rgba(29,158,117,0.1);color:#1D9E75;border:0.5px solid rgba(29,158,117,0.3);white-space:nowrap;font-family:'IBM Plex Mono',monospace}}
  .badge-mapa{{font-size:8px;padding:1px 5px;border-radius:2px;background:rgba(124,106,247,0.1);color:#7c6af7;border:0.5px solid rgba(124,106,247,0.3);white-space:nowrap;font-family:'IBM Plex Mono',monospace}}
  .badge-doc{{font-size:8px;padding:1px 5px;border-radius:2px;background:#f0f0e8;color:#888;border:0.5px solid #ddd;white-space:nowrap;font-family:'IBM Plex Mono',monospace}}
  .rel-tags{{margin-top:7px;display:flex;flex-wrap:wrap;gap:4px}}
  .rel-tag{{font-size:8px;padding:1px 6px;border-radius:3px;font-family:'IBM Plex Mono',monospace}}
  .rel-tag.reg{{background:rgba(55,138,221,0.08);color:#378ADD;border:0.5px solid rgba(55,138,221,0.25)}}
  .rel-tag.rev{{background:rgba(226,75,74,0.07);color:#E24B4A;border:0.5px solid rgba(226,75,74,0.2)}}
  .rel-tag.revp{{background:rgba(186,117,23,0.08);color:#BA7517;border:0.5px solid rgba(186,117,23,0.25)}}
  /* RELACIONAMENTOS */
  .rel-row{{display:flex;align-items:center;gap:8px;margin-bottom:6px;font-size:10px;flex-wrap:wrap}}
  .rel-lei{{font-family:'IBM Plex Mono',monospace;font-size:9px;background:#f0f0e8;border:0.5px solid #ddd;border-radius:3px;padding:2px 6px;color:#1a1a1a}}
  .rel-seta{{color:#aaa;font-size:9px}}
  .rel-desc{{font-size:9px;color:#777}}
  /* NÃO ENCONTRADAS */
  .nenc-row{{display:flex;justify-content:space-between;align-items:center;padding:7px 10px;background:#f8f8f4;border-radius:5px;margin-bottom:5px;border:0.5px solid #e0e0d8}}
  .nenc-lei{{font-family:'IBM Plex Mono',monospace;font-size:10px;color:#1a1a1a}}
  .nenc-orig{{font-size:9px;color:#aaa;margin-top:1px}}
  /* FONTES */
  .fonte-row{{display:flex;align-items:center;gap:10px;padding:5px 0;border-bottom:0.5px solid #f0f0e8;font-size:10px}}
  .fonte-row:last-child{{border-bottom:none}}
  .fonte-dot{{width:8px;height:8px;border-radius:50%;flex-shrink:0}}
  /* FOOTER */
  .footer{{background:#f0f0e8;padding:8px 28px;display:flex;justify-content:space-between;font-size:8px;color:#aaa;font-family:'IBM Plex Mono',monospace;border-top:1px solid #e0e0d8;margin-top:20px}}
</style>
</head><body><div class="page">
'''

    # ── CAPA ──────────────────────────────────────────────────────
    html += f'''
<div class="capa-bar">
  <div class="capa-logo">URBANLEX</div>
  <div class="capa-tag">Relatório gerado por IA</div>
</div>
<div class="capa-title">
  <div class="capa-mun">{_esc(municipio)} / {_esc(estado)}</div>
  <div class="capa-sub">Levantamento de legislação urbanística municipal vigente</div>
  <div class="capa-meta">
    <span class="meta-item"><strong>Gerado em</strong> {agora}</span>
    {"<span class='meta-item'><strong>Tempo</strong> " + tempo_str + "</span>" if tempo_str else ""}
    {"<span class='meta-item'><strong>Custo</strong> " + custo_str + "</span>" if custo_usd else ""}
    {"<span class='meta-item'><strong>Tokens</strong> " + tokens_str + "</span>" if tokens_str else ""}
  </div>
</div>
'''

    # ── 01 RESUMO EXECUTIVO ───────────────────────────────────────
    html += '<div class="sec-divider">01 — resumo executivo</div><div class="sec">'
    html += f'''<div class="sec-label">Visão geral</div>
<div class="cards">
  <div class="card"><div class="card-num ok">{len(enc)}</div><div class="card-lbl">legislações encontradas</div></div>
  <div class="card"><div class="card-num {"warn" if nao_loc else "ok"}">{len(nao_loc)}</div><div class="card-lbl">não localizadas</div></div>
  <div class="card"><div class="card-num">{sum(len(l.get("anexos_lm") or []) for l in enc)}</div><div class="card-lbl">anexos analisados</div></div>
  <div class="card"><div class="card-num">{len(cats_enc)}</div><div class="card-lbl">categorias cobertas</div></div>
</div>
'''

    # Alertas
    html += '<div class="sec-label" style="margin-top:10px">Alertas</div>'
    # Verificar revogações parciais
    revp_count = sum(1 for l in enc if l.get('revoga_parcialmente') or l.get('revogado_parcialmente_por'))
    if revp_count:
        html += f'<div class="alert warn"><div class="alert-icon">▲</div><div><strong>Revogações parciais:</strong> {revp_count} legislação(ões) com revogações parciais identificadas. Verificar artigos e partes afetadas.</div></div>'
    if nao_loc:
        html += f'<div class="alert danger"><div class="alert-icon">⚠</div><div><strong>{len(nao_loc)} legislação(ões) não localizada(s)</strong> foram identificadas pelo Gemini mas não encontradas em nenhuma fonte consultada.</div></div>'
    if plantas_encontradas:
        html += f'<div class="alert ok"><div class="alert-icon">✓</div><div><strong>Documentos cartográficos encontrados:</strong> {len([p for p in plantas_encontradas if "tipo" not in p])} mapa(s) de zoneamento e {len([p for p in plantas_encontradas if p.get("tipo")=="tabela"])} tabela(s) de parâmetros localizados.</div></div>'
    if not revp_count and not nao_loc and not plantas_nao_encontradas:
        html += '<div class="alert ok"><div class="alert-icon">✓</div><div>Busca concluída sem alertas críticos.</div></div>'
    html += '</div>'

    # ── 02 PLANTAS E MAPAS ────────────────────────────────────────
    html += '<div class="sec-divider">02 — plantas, mapas e tabelas de parâmetros</div><div class="sec">'
    html += '<div class="sec-label">Status dos documentos cartográficos e técnicos</div>'

    if plantas_encontradas:
        for p in plantas_encontradas:
            tipo_p = p.get('tipo','mapa')
            icone = '📊' if tipo_p == 'tabela' else '🗺'
            label = 'Tabela de Parâmetros' if tipo_p == 'tabela' else 'Mapa de Zoneamento'
            html += f'''<div class="planta-row">
  <div class="planta-icon found">{icone}</div>
  <div style="flex:1">
    <div class="planta-title">{label} — {_esc(p["lei"])} <span class="pill ok">Encontrado</span></div>
    <div class="planta-desc">{_esc(p["anx"])}</div>
  </div>
</div>'''
    else:
        html += '<div class="planta-row nok"><div class="planta-icon miss">🗺</div><div style="flex:1"><div class="planta-title">Nenhum mapa ou tabela de parâmetros localizado como anexo <span class="pill nok">Não encontrado</span></div><div class="planta-desc">Os parâmetros urbanísticos podem estar no corpo da lei ou em decretos não localizados</div></div></div>'

    for n in plantas_nao_encontradas:
        html += f'''<div class="planta-row nok">
  <div class="planta-icon miss">🗺</div>
  <div style="flex:1">
    <div class="planta-title">{_esc(n.get("tipo",""))} {_esc(n.get("numero",""))}/{_esc(n.get("ano",""))} <span class="pill nok">Não localizado</span></div>
    <div class="planta-desc">{_esc(n.get("descricao",""))} — identificado pelo Gemini mas não encontrado</div>
  </div>
</div>'''
    html += '</div>'

    # ── 03 LEGISLAÇÕES POR CATEGORIA ─────────────────────────────
    html += '<div class="sec-divider">03 — legislações encontradas</div><div class="sec">'

    # Legenda categorias
    html += '<div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:12px">'
    for cat, cor in CORES_CAT.items():
        if cat in cats_enc:
            html += f'<span style="display:flex;align-items:center;gap:5px;font-size:9px;color:#666"><span style="width:8px;height:8px;border-radius:2px;background:{cor};display:inline-block"></span>{LABELS_CAT[cat]}</span>'
    html += '</div>'

    # Agrupar por categoria
    by_cat = {}
    for leg in enc:
        cat = leg.get('categoria','outros') if isinstance(leg, dict) else 'outros'
        by_cat.setdefault(cat, []).append(leg)

    cat_order = ['plano_diretor','zoneamento','uso_ocupacao','parcelamento','codigo_obras','outros']
    for cat in cat_order:
        legs_cat = by_cat.get(cat, [])
        if not legs_cat:
            continue
        for leg in legs_cat:
            cor = CORES_CAT.get(cat, '#888')
            tipo = _esc(leg.get('tipo',''))
            num  = _esc(leg.get('numero',''))
            ano  = _esc(leg.get('ano',''))
            desc = _esc(leg.get('descricao','') or leg.get('ementa',''))
            link = leg.get('link','') or leg.get('url','')

            html += f'<div class="leg-row"><div class="leg-strip" style="background:{cor}"></div><div class="leg-body">'
            html += f'<div class="leg-title">{tipo} {num}/{ano}'
            if link:
                html += f' <span class="pill ok">Encontrada</span>'
            html += '</div>'
            if desc:
                html += f'<div class="leg-desc">{desc[:200]}{"..." if len(desc)>200 else ""}</div>'

            # Anexos
            anexos = leg.get('anexos_lm') or []
            if anexos:
                html += f'<div class="sec-label" style="margin-top:6px;margin-bottom:4px">Anexos ({len(anexos)})</div><div class="anx-list">'
                for anx in anexos[:8]:
                    nome_anx = anx.get('nome','') if isinstance(anx, dict) else str(anx)
                    badge = _anexo_badge(nome_anx.lower())
                    html += f'<div class="anx-row">{badge}<span>{_esc(nome_anx[:100])}</span></div>'
                if len(anexos) > 8:
                    html += f'<div class="anx-row"><span class="badge-doc">+{len(anexos)-8}</span><span>outros anexos</span></div>'
                html += '</div>'

            # Tags de relações
            tags = []
            for r in (leg.get('altera') or []):
                tags.append(f'<span class="rel-tag reg">regulamenta/altera: {_esc(str(r)[:40])}</span>')
            for r in (leg.get('revoga') or []):
                tags.append(f'<span class="rel-tag rev">revoga: {_esc(str(r)[:40])}</span>')
            for r in (leg.get('revoga_parcialmente') or []):
                lei_r = r.get('lei','') if isinstance(r,dict) else str(r)
                tags.append(f'<span class="rel-tag revp">revoga parc.: {_esc(lei_r[:40])}</span>')
            for r in (leg.get('revogado_parcialmente_por') or []):
                lei_r = r.get('lei','') if isinstance(r,dict) else str(r)
                tags.append(f'<span class="rel-tag revp">rev. parc. por: {_esc(lei_r[:40])}</span>')
            for r in (leg.get('alterado_por') or leg.get('citado_em') or []):
                tags.append(f'<span class="rel-tag reg">regulamentada por: {_esc(str(r)[:40])}</span>')
            if tags:
                html += '<div class="rel-tags">' + ''.join(tags[:6]) + '</div>'
            html += '</div></div>'

    html += '</div>'

    # ── 04 MAPA DE RELACIONAMENTOS ────────────────────────────────
    rels = []
    for leg in enc:
        tipo_n = f"{leg.get('tipo','')} {leg.get('numero','')}/{leg.get('ano','')}"
        for r in (leg.get('revoga') or []):
            rels.append((tipo_n, 'revoga totalmente', str(r), 'rev'))
        for r in (leg.get('revoga_parcialmente') or []):
            lei_r = r.get('lei','') if isinstance(r,dict) else str(r)
            partes = r.get('partes','') if isinstance(r,dict) else ''
            rels.append((tipo_n, f'revoga parcialmente{(" ("+partes[:50]+")" if partes else "")}', lei_r, 'revp'))
        for r in (leg.get('altera') or []):
            rels.append((tipo_n, 'regulamenta/altera', str(r), 'reg'))

    if rels:
        html += '<div class="sec-divider">04 — mapa de relacionamentos</div><div class="sec">'
        html += '<div class="sec-label">Genealogia legislativa</div>'
        for orig, rel_tipo, dest, cls in rels[:20]:
            color = '#E24B4A' if cls=='rev' else '#BA7517' if cls=='revp' else '#378ADD'
            html += f'<div class="rel-row"><span class="rel-lei">{_esc(orig[:50])}</span><span class="rel-seta" style="color:{color}">──{_esc(rel_tipo)}──▶</span><span class="rel-lei">{_esc(dest[:50])}</span></div>'
        html += '</div>'

    # ── 05 NÃO ENCONTRADAS ────────────────────────────────────────
    if nao_loc:
        html += '<div class="sec-divider">05 — legislações não localizadas</div><div class="sec">'
        html += '<div class="sec-label">Identificadas pelo Gemini mas não encontradas em nenhuma fonte</div>'
        for n in nao_loc:
            html += f'''<div class="nenc-row">
  <div><div class="nenc-lei">{_esc(n.get("tipo",""))} {_esc(n.get("numero",""))}/{_esc(n.get("ano",""))}</div>
  <div class="nenc-orig">{_esc(n.get("descricao","") or "—")}</div></div>
  <span class="pill nok">Não localizado</span>
</div>'''
        html += '</div>'

    # ── 06 FONTES ─────────────────────────────────────────────────
    html += f'<div class="sec-divider">{"06" if nao_loc else "05"} — fontes consultadas</div><div class="sec">'
    html += '''<div class="fonte-row"><div class="fonte-dot" style="background:#1D9E75"></div><span style="min-width:180px;font-weight:500">LeisMunicipais.com.br</span><span style="color:#666">Fonte principal — repositório nacional de legislação municipal</span></div>
<div class="fonte-row"><div class="fonte-dot" style="background:#378ADD"></div><span style="min-width:180px;font-weight:500">1º Fallback — Google</span><span style="color:#666">Query formal com avaliação de snippet (até 5 resultados)</span></div>
<div class="fonte-row"><div class="fonte-dot" style="background:#BA7517"></div><span style="min-width:180px;font-weight:500">2º Fallback — Portal câmara/pref.</span><span style="color:#666">Busca no portal legislativo municipal (até 10 passos)</span></div>
<div class="fonte-row"><div class="fonte-dot" style="background:#7c6af7"></div><span style="min-width:180px;font-weight:500">Gemini + Google Search</span><span style="color:#666">6 perguntas de identificação inicial com busca web em tempo real</span></div>
'''
    html += '</div>'

    # ── FOOTER ────────────────────────────────────────────────────
    html += f'<div class="footer"><span>UrbanLex · Relatório gerado automaticamente por IA</span><span>{agora} · {_esc(municipio)}/{_esc(estado)}</span></div>'
    html += '</div></body></html>'
    return html


def gerar_relatorio_pdf(resultado, municipio, estado, custo_usd=None, token_stats=None, tempo_segundos=None, nao_encontradas=None, logs=None):
    """Gera PDF do relatório. Retorna (path_pdf, url_pdf) ou (None, None) em caso de erro."""
    import unicodedata, re as _re

    def slug(s):
        s = unicodedata.normalize('NFKD', s).encode('ascii','ignore').decode()
        return _re.sub(r'[^A-Za-z0-9_]','_',s).strip('_')[:40]

    nome = f"relatorio_{slug(municipio)}_{slug(estado)}.pdf"
    path = f"/var/www/urbanlex/static/downloads/{nome}"
    url  = f"/static/downloads/{nome}"

    html_content = gerar_html_relatorio(
        resultado, municipio, estado,
        custo_usd=custo_usd, token_stats=token_stats,
        tempo_segundos=tempo_segundos, nao_encontradas=nao_encontradas
    )

    # Tentar WeasyPrint
    try:
        import weasyprint
        import tempfile as _tf
        with _tf.NamedTemporaryFile(suffix='.html', mode='w', encoding='utf-8', delete=False) as _tmp:
            _tmp.write(html_content)
            _tmp_path = _tmp.name
        weasyprint.HTML(filename=_tmp_path).write_pdf(path)
        import os as _os_wp; _os_wp.unlink(_tmp_path)
        if logs is not None:
            logs.append({'nivel':'ok','msg':f'📄 Relatório PDF gerado: {nome}'})
        return path, url
    except Exception as e1:
        if logs is not None:
            logs.append({'nivel':'aviso','msg':f'WeasyPrint falhou ({str(e1)[:60]}), tentando ReportLab...'})

    # Fallback: salvar HTML como arquivo (abrível pelo browser)
    try:
        html_path = path.replace('.pdf','.html')
        html_url  = url.replace('.pdf','.html')
        with open(html_path,'w',encoding='utf-8') as f:
            f.write(html_content)
        if logs is not None:
            logs.append({'nivel':'ok','msg':f'📄 Relatório HTML gerado: {os.path.basename(html_path)}'})
        return html_path, html_url
    except Exception as e2:
        if logs is not None:
            logs.append({'nivel':'aviso','msg':f'Relatório: erro — {str(e2)[:80]}'})
        return None, None

def gerar_tabela_pdf(resultado, municipio, estado, logs=None):
    """Gera PDF da tabela completa de legislações (equivalente ao baixarTabelaPDF do frontend)."""
    import os, re, datetime
    from weasyprint import HTML as WP_HTML

    COLS = [
        ('estado','Estado'),('municipio','Município'),('tipo','Tipo'),('numero','Nº'),
        ('ano','Ano'),('ementa','Ementa'),('pergunta','Pergunta'),('status','Status'),
        ('altera','Regulamenta/Altera'),('alterado_por','Regulamentado por'),
        ('revoga','Revoga'),('revogado_por','Revogado por'),
        ('revoga_parcialmente','Revoga parcialmente'),
        ('revogado_parcialmente_por','Revogada parcialmente por?'),
        ('cita','Faz referência à'),('citado_em','Referenciada em'),('link','Link'),
    ]

    # Coletar todas as legislações da tabela (todas as entradas incluindo revogadas/nao_encontradas)
    todas = resultado.get('tabela_legislacoes') or resultado.get('legislacoes') or []
    if not todas:
        todas = resultado.get('encontradas') or []

    agora = datetime.datetime.now().strftime('%d/%m/%Y %H:%M')

    def fmt_val(key, val):
        if isinstance(val, list):
            if not val: return '–'
            if isinstance(val[0], dict):
                return '<br>'.join(f"{o.get('lei','')}{ ': '+o['partes'] if o.get('partes') else ''}" for o in val)
            return '<br>'.join(str(v) for v in val)
        if not val: return '–'
        if key == 'status':
            cor = '#4ade80' if val=='encontrada' else '#f87171' if val=='nao_encontrada' else '#fbbf24'
            return f'<span style="color:{cor};font-weight:700">{val}</span>'
        if key == 'link' and val != '–':
            return f'<a href="{val}" style="color:#60a5fa;font-size:9px">{str(val)[:40]}...</a>'
        return str(val)

    linhas = ''
    for i, r in enumerate(todas):
        bg = '#1a1a2e' if i % 2 == 0 else '#16213e'
        linhas += f'<tr style="background:{bg}">'
        for key, _ in COLS:
            val = r.get(key, '')
            linhas += f'<td style="padding:5px 8px;border:1px solid #2d3748;font-size:9px;color:#e2e8f0;vertical-align:top">{fmt_val(key, val)}</td>'
        linhas += '</tr>'

    cabecalho = ''.join(f'<th>{lbl}</th>' for _, lbl in COLS)

    html = f'''<!DOCTYPE html><html><head><meta charset="UTF-8">
<title>Legislações - {municipio}/{estado}</title>
<style>
  body{{font-family:Arial,sans-serif;background:#0f172a;color:#e2e8f0;margin:16px;-webkit-print-color-adjust:exact;print-color-adjust:exact}}
  h1{{color:#7c3aed;font-size:15px;margin-bottom:2px}}
  .sub{{font-size:10px;color:#94a3b8;margin-bottom:12px}}
  table{{width:100%;border-collapse:collapse;table-layout:fixed}}
  th{{background:#7c3aed;color:white;padding:6px 8px;font-size:9px;text-align:left;border:1px solid #4c1d95;word-wrap:break-word}}
  td{{word-wrap:break-word;overflow-wrap:break-word}}
  @page{{size:A3 landscape;margin:12mm}}
</style></head><body>
<h1>📋 Legislações Identificadas — {municipio} / {estado}</h1>
<div class="sub">Gerado em {agora} · {len(todas)} legislação(ões)</div>
<table><thead><tr>{cabecalho}</tr></thead><tbody>{linhas}</tbody></table>
</body></html>'''

    slug = re.sub(r'[^a-zA-Z0-9_]', '_', f'{estado}_{municipio}')
    nome = f'tabela_legislacoes_{slug}.pdf'
    path = f'/var/www/urbanlex/static/downloads/{nome}'
    url  = f'/static/downloads/{nome}'
    try:
        WP_HTML(string=html).write_pdf(path)
        if logs is not None:
            logs.append({'nivel':'ok','msg':f'📋 Tabela PDF gerada: {nome}'})
        return path, url
    except Exception as e:
        if logs is not None:
            logs.append({'nivel':'aviso','msg':f'Tabela PDF erro: {str(e)[:80]}'})
        return None, None
