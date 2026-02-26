/**
 * UrbanLex v3.5 - Legislacoes Manager
 * Gerenciamento de legislacoes: edicao inline, download, gestao multi-arquivo
 *
 * Uso: Incluir no template legislacoes.html
 *   <script src="/static/js/legislacoes_manager.js"></script>
 *   <script>LegManager.init();</script>
 */
const LegManager = (() => {

  // =====================================================================
  //  UTILIDADES
  // =====================================================================
  const API = (url, opts = {}) => {
    const defaults = { headers: {} };
    if (opts.body && !(opts.body instanceof FormData)) {
      defaults.headers['Content-Type'] = 'application/json';
      opts.body = JSON.stringify(opts.body);
    }
    return fetch(url, { ...defaults, ...opts })
      .then(r => r.ok ? r.json() : r.json().then(e => Promise.reject(e)))
      .catch(err => {
        console.error('API error:', err);
        throw err;
      });
  };

  const fmtBytes = (b) => {
    if (!b || b === 0) return '0 B';
    const u = ['B', 'KB', 'MB', 'GB'];
    const i = Math.min(Math.floor(Math.log(b) / Math.log(1024)), u.length - 1);
    return (b / Math.pow(1024, i)).toFixed(i > 0 ? 1 : 0) + ' ' + u[i];
  };

  const fmtDate = (d) => {
    if (!d) return '-';
    const dt = new Date(d);
    return dt.toLocaleDateString('pt-BR') + ' ' + dt.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' });
  };

  const escHtml = (s) => {
    const d = document.createElement('div');
    d.textContent = s || '';
    return d.innerHTML;
  };

  let _tipos = null, _assuntos = null;
  const loadSelects = async () => {
    if (!_tipos)    _tipos    = (await API('/api/config/tipos-legislacao')).data || [];
    if (!_assuntos) _assuntos = (await API('/api/config/assuntos')).data || [];
  };

  const toast = (msg, type = 'success') => {
    const t = document.createElement('div');
    t.className = `leg-toast leg-toast--${type}`;
    t.textContent = msg;
    document.body.appendChild(t);
    requestAnimationFrame(() => t.classList.add('leg-toast--show'));
    setTimeout(() => { t.classList.remove('leg-toast--show'); setTimeout(() => t.remove(), 300); }, 3000);
  };

  // =====================================================================
  //  CSS (injetado dinamicamente)
  // =====================================================================
  const injectCSS = () => {
    if (document.getElementById('leg-mgr-css')) return;
    const style = document.createElement('style');
    style.id = 'leg-mgr-css';
    style.textContent = `
      /* ---- Icones de acao na tabela ---- */
      .leg-actions { display:flex; gap:4px; align-items:center; flex-shrink:0; flex-wrap:nowrap; white-space:nowrap; }
      .leg-actions button {
        background:none; border:none; cursor:pointer; padding:3px 4px;
        border-radius:6px; transition:all .15s; color:#64748b; display:inline-flex;
        align-items:center; justify-content:center; flex-shrink:0;
      }
      .leg-actions button:hover { background:#f1f5f9; color:#0f172a; }
      .leg-actions button.leg-act-edit:hover   { color:#2563eb; background:#eff6ff; }
      .leg-actions button.leg-act-dl:hover     { color:#059669; background:#ecfdf5; }
      .leg-actions button.leg-act-files:hover  { color:#7c3aed; background:#f5f3ff; }
      .leg-actions button.leg-act-save         { color:#059669; }
      .leg-actions button.leg-act-save:hover   { background:#ecfdf5; }
      .leg-actions button.leg-act-cancel       { color:#dc2626; }
      .leg-actions button.leg-act-cancel:hover { background:#fef2f2; }
      .leg-actions svg { width:16px; height:16px; }

      /* Badge de contagem de arquivos */
      .leg-file-badge {
        font-size:10px; background:#7c3aed; color:#fff; border-radius:50%;
        width:16px; height:16px; display:inline-flex; align-items:center;
        justify-content:center; margin-left:2px; font-weight:600;
      }

      /* ---- Edicao inline ---- */
      .leg-row--editing td { background:#fffbeb !important; }
      .leg-edit-input {
        width:100%; padding:4px 8px; border:1px solid #d1d5db; border-radius:6px;
        font-size:13px; background:#fff; color:#1e293b; transition:border-color .15s;
      }
      .leg-edit-input:focus { outline:none; border-color:#2563eb; box-shadow:0 0 0 2px rgba(37,99,235,.15); }
      select.leg-edit-input { padding:4px 6px; color:#1e293b; }

      /* ---- Modal ---- */
      .leg-modal-overlay {
        position:fixed; inset:0; background:rgba(15,23,42,.45); z-index:9998;
        display:flex; align-items:center; justify-content:center;
        opacity:0; transition:opacity .2s; backdrop-filter:blur(2px);
      }
      .leg-modal-overlay.leg-modal--show { opacity:1; }
      .leg-modal {
        background:#fff; border-radius:16px; width:95%; max-width:640px;
        max-height:85vh; display:flex; flex-direction:column;
        box-shadow:0 25px 50px -12px rgba(0,0,0,.25);
        transform:translateY(12px); transition:transform .2s;
      }
      .leg-modal-overlay.leg-modal--show .leg-modal { transform:translateY(0); }

      .leg-modal-header {
        padding:20px 24px 16px; border-bottom:1px solid #e2e8f0;
        display:flex; justify-content:space-between; align-items:center;
      }
      .leg-modal-header h3 { margin:0; font-size:17px; font-weight:600; color:#0f172a; }
      .leg-modal-close {
        background:none; border:none; cursor:pointer; color:#94a3b8;
        padding:4px; border-radius:8px; transition:all .15s;
      }
      .leg-modal-close:hover { background:#f1f5f9; color:#0f172a; }

      .leg-modal-body { padding:20px 24px; overflow-y:auto; flex:1; }

      .leg-modal-footer {
        padding:16px 24px; border-top:1px solid #e2e8f0;
        display:flex; justify-content:space-between; align-items:center;
      }

      /* Barra de espaco */
      .leg-space-bar {
        width:100%; height:8px; background:#e2e8f0; border-radius:99px;
        overflow:hidden; margin:8px 0;
      }
      .leg-space-bar-fill {
        height:100%; border-radius:99px; transition:width .3s;
        background:linear-gradient(90deg, #059669, #10b981);
      }
      .leg-space-bar-fill.leg-space--warning { background:linear-gradient(90deg, #d97706, #f59e0b); }
      .leg-space-bar-fill.leg-space--danger  { background:linear-gradient(90deg, #dc2626, #ef4444); }
      .leg-space-info { font-size:12px; color:#64748b; margin-bottom:12px; }

      /* Lista de arquivos */
      .leg-file-list { list-style:none; padding:0; margin:0 0 16px; }
      .leg-file-item {
        display:flex; align-items:center; gap:10px; padding:10px 12px;
        border:1px solid #e2e8f0; border-radius:10px; margin-bottom:8px;
        transition:all .15s;
      }
      .leg-file-item:hover { border-color:#cbd5e1; background:#f8fafc; }
      .leg-file-icon {
        width:36px; height:36px; border-radius:8px; display:flex;
        align-items:center; justify-content:center; flex-shrink:0;
        font-size:11px; font-weight:700; text-transform:uppercase; color:#fff;
      }
      .leg-file-icon--pdf  { background:#dc2626; }
      .leg-file-icon--doc  { background:#2563eb; }
      .leg-file-icon--xls  { background:#059669; }
      .leg-file-icon--img  { background:#7c3aed; }
      .leg-file-icon--other{ background:#64748b; }
      .leg-file-info { flex:1; min-width:0; }
      .leg-file-name {
        font-size:13px; font-weight:500; color:#1e293b;
        white-space:nowrap; overflow:hidden; text-overflow:ellipsis;
      }
      .leg-file-meta { font-size:11px; color:#94a3b8; margin-top:2px; }
      .leg-file-actions { display:flex; gap:4px; }
      .leg-file-actions button {
        background:none; border:none; cursor:pointer; padding:6px;
        border-radius:6px; color:#94a3b8; transition:all .15s;
      }
      .leg-file-actions button:hover { background:#f1f5f9; color:#0f172a; }
      .leg-file-actions button.leg-file-del:hover { color:#dc2626; background:#fef2f2; }

      /* Upload area */
      .leg-upload-zone {
        border:2px dashed #cbd5e1; border-radius:12px; padding:24px;
        text-align:center; cursor:pointer; transition:all .2s;
        margin-bottom:8px;
      }
      .leg-upload-zone:hover, .leg-upload-zone.leg-upload--drag {
        border-color:#2563eb; background:#eff6ff;
      }
      .leg-upload-zone p { margin:8px 0 0; font-size:13px; color:#64748b; }
      .leg-upload-zone .leg-upload-icon { font-size:28px; color:#94a3b8; }
      .leg-upload-zone input { display:none; }
      .leg-upload-limit { font-size:11px; color:#94a3b8; text-align:center; }

      /* Botao primario */
      .leg-btn-primary {
        background:#2563eb; color:#fff; border:none; padding:8px 16px;
        border-radius:8px; font-size:13px; font-weight:500; cursor:pointer;
        transition:all .15s;
      }
      .leg-btn-primary:hover { background:#1d4ed8; }
      .leg-btn-primary:disabled { opacity:.5; cursor:not-allowed; }

      /* Spinner */
      .leg-spinner {
        display:inline-block; width:14px; height:14px;
        border:2px solid #e2e8f0; border-top-color:#2563eb;
        border-radius:50%; animation:leg-spin .6s linear infinite;
      }
      @keyframes leg-spin { to { transform:rotate(360deg); } }

      /* Empty state */
      .leg-empty { text-align:center; padding:24px; color:#94a3b8; font-size:13px; }

      /* Toast */
      .leg-toast {
        position:fixed; bottom:24px; right:24px; padding:12px 20px;
        border-radius:10px; font-size:13px; font-weight:500; z-index:9999;
        transform:translateY(20px); opacity:0; transition:all .3s;
        box-shadow:0 8px 24px rgba(0,0,0,.15);
      }
      .leg-toast--show { transform:translateY(0); opacity:1; }
      .leg-toast--success { background:#059669; color:#fff; }
      .leg-toast--error   { background:#dc2626; color:#fff; }
      .leg-toast--info    { background:#2563eb; color:#fff; }
    `;
    document.head.appendChild(style);
  };

  // =====================================================================
  //  SVG ICONS
  // =====================================================================
  const ICONS = {
    edit: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>',
    download: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>',
    files: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>',
    save: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/></svg>',
    cancel: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>',
    close: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>',
    trash: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/></svg>',
    upload: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>',
    dlFile: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>',
  };

  // =====================================================================
  //  RENDERIZAR ICONES DE ACAO (chamado pelo template para cada row)
  // =====================================================================
  const renderActions = (legId, opts = {}) => {
    const hasFiles = opts.qtdArquivos > 0;
    const hasDoc   = opts.hasDocumento;
    return `
      <div class="leg-actions" data-leg-id="${legId}">
        <button class="leg-act-edit" onclick="LegManager.startEdit(${legId})" title="Editar">
          ${ICONS.edit}
        </button>
        <button class="leg-act-dl" onclick="LegManager.download(${legId})" title="Download"
                ${!hasDoc && !hasFiles ? 'disabled style="opacity:.3;cursor:not-allowed"' : ''}>
          ${ICONS.download}
        </button>
        <button class="leg-act-files" onclick="LegManager.openFiles(${legId})" title="Arquivos">
          ${ICONS.files}
          ${hasFiles ? `<span class="leg-file-badge">${opts.qtdArquivos}</span>` : ''}
        </button>
      </div>`;
  };

  // =====================================================================
  //  EDICAO INLINE
  // =====================================================================
  let _editingId = null;
  let _originalData = null;

  const startEdit = async (legId) => {
    if (_editingId) cancelEdit();
    await loadSelects();

    const res = await API(`/api/legislacoes/${legId}`);
    if (!res.success) return toast('Erro ao carregar legislacao', 'error');
    _originalData = res.data;
    _editingId = legId;

    const row = document.querySelector(`tr[data-leg-id="${legId}"], [data-leg-row="${legId}"]`);
    if (!row) return;
    row.classList.add('leg-row--editing');

    // Substituir celulas editaveis por inputs
    const editableCells = row.querySelectorAll('[data-field]');
    editableCells.forEach(cell => {
      const field = cell.dataset.field;
      const val = _originalData[field] ?? '';
      cell.dataset.originalHtml = cell.innerHTML;

      if (field === 'tipo_id') {
        cell.innerHTML = `<select class="leg-edit-input" data-edit-field="${field}">
          <option value="">--</option>
          ${(_tipos||[]).map(t => `<option value="${t.id}" ${t.id == val ? 'selected' : ''}>${escHtml(t.nome)}</option>`).join('')}
        </select>`;
      } else if (field === 'assunto_id') {
        cell.innerHTML = `<select class="leg-edit-input" data-edit-field="${field}">
          <option value="">--</option>
          ${(_assuntos||[]).map(a => `<option value="${a.id}" ${a.id == val ? 'selected' : ''}>${escHtml(a.nome)}</option>`).join('')}
        </select>`;
      } else if (field === 'status') {
        cell.innerHTML = `<select class="leg-edit-input" data-edit-field="${field}">
          ${['vigente','revogada','alterada','suspensa'].map(s => `<option value="${s}" ${s === val ? 'selected' : ''}>${s.charAt(0).toUpperCase()+s.slice(1)}</option>`).join('')}
        </select>`;
      } else if (field === 'em_monitoramento') {
        cell.innerHTML = `<select class="leg-edit-input" data-edit-field="${field}">
          <option value="true" ${val ? 'selected' : ''}>Sim</option>
          <option value="false" ${!val ? 'selected' : ''}>Nao</option>
        </select>`;
      } else if (field === 'data_publicacao') {
        const dateVal = val ? val.substring(0, 10) : '';
        cell.innerHTML = `<input type="date" class="leg-edit-input" data-edit-field="${field}" value="${dateVal}">`;
      } else {
        cell.innerHTML = `<input type="text" class="leg-edit-input" data-edit-field="${field}" value="${escHtml(String(val))}">`;
      }
    });

    // Trocar botoes de acao
    const actionsDiv = row.querySelector('.leg-actions');
    if (actionsDiv) {
      actionsDiv.innerHTML = `
        <button class="leg-act-save" onclick="LegManager.saveEdit(${legId})" title="Salvar">
          ${ICONS.save}
        </button>
        <button class="leg-act-cancel" onclick="LegManager.cancelEdit()" title="Cancelar">
          ${ICONS.cancel}
        </button>`;
    }
  };

  const saveEdit = async (legId) => {
    const row = document.querySelector(`tr[data-leg-id="${legId}"], [data-leg-row="${legId}"]`);
    if (!row) return;

    const inputs = row.querySelectorAll('[data-edit-field]');
    const payload = {};
    inputs.forEach(input => {
      const field = input.dataset.editField;
      let val = input.value;
      if (field === 'ano') val = val ? parseInt(val) : null;
      else if (field === 'tipo_id' || field === 'assunto_id') val = val ? parseInt(val) : null;
      else if (field === 'em_monitoramento') val = val === 'true';
      else if (val === '') val = null;
      payload[field] = val;
    });

    // Verificar se algo mudou
    let changed = false;
    for (const k of Object.keys(payload)) {
      if (String(payload[k] ?? '') !== String(_originalData[k] ?? '')) { changed = true; break; }
    }
    if (!changed) { cancelEdit(); return; }

    try {
      const res = await API(`/api/legislacoes/${legId}`, { method: 'PUT', body: payload });
      if (!res.success) throw new Error(res.error || 'Erro ao salvar');
      toast('Legislacao atualizada');
      cancelEdit();
      // Recarregar a linha/tabela (disparar evento custom)
      document.dispatchEvent(new CustomEvent('legUpdated', { detail: { id: legId, data: res.data } }));
      // Se ha funcao global de reload, chamar
      if (typeof carregarLegislacoes === 'function') carregarLegislacoes();
    } catch (err) {
      toast(err.message || 'Erro ao salvar', 'error');
    }
  };

  const cancelEdit = () => {
    if (!_editingId) return;
    const row = document.querySelector(`tr[data-leg-id="${_editingId}"], [data-leg-row="${_editingId}"]`);
    if (row) {
      row.classList.remove('leg-row--editing');
      row.querySelectorAll('[data-field]').forEach(cell => {
        if (cell.dataset.originalHtml !== undefined) {
          cell.innerHTML = cell.dataset.originalHtml;
          delete cell.dataset.originalHtml;
        }
      });
      // Restaurar botoes de acao
      const actionsDiv = row.querySelector('.leg-actions');
      if (actionsDiv && _originalData) {
        actionsDiv.outerHTML = renderActions(_editingId, {
          qtdArquivos: _originalData.qtd_arquivos || _originalData.arquivos?.length || 0,
          hasDocumento: !!_originalData.arquivo_url
        });
      }
    }
    _editingId = null;
    _originalData = null;
  };

  // =====================================================================
  //  DOWNLOAD
  // =====================================================================
  const download = (legId) => {
    window.open(`/api/legislacoes/${legId}/documento`, '_blank');
  };

  const downloadArquivo = (legId, arqId) => {
    window.open(`/api/legislacoes/${legId}/arquivos/${arqId}/download`, '_blank');
  };

  // =====================================================================
  //  MODAL DE ARQUIVOS
  // =====================================================================
  let _modalLegId = null;
  let _modalOverlay = null;

  const getFileIconClass = (tipo) => {
    if (!tipo) return 'leg-file-icon--other';
    tipo = tipo.toLowerCase();
    if (['pdf'].includes(tipo)) return 'leg-file-icon--pdf';
    if (['doc','docx','odt','rtf'].includes(tipo)) return 'leg-file-icon--doc';
    if (['xls','xlsx','csv','ods'].includes(tipo)) return 'leg-file-icon--xls';
    if (['jpg','jpeg','png','gif','webp','svg','bmp'].includes(tipo)) return 'leg-file-icon--img';
    return 'leg-file-icon--other';
  };

  const getFileIconLabel = (tipo) => {
    if (!tipo) return '?';
    return tipo.substring(0, 4).toUpperCase();
  };

  const openFiles = async (legId) => {
    _modalLegId = legId;
    closeModal();

    // Carregar dados
    const res = await API(`/api/legislacoes/${legId}/arquivos`);
    if (!res.success) return toast('Erro ao carregar arquivos', 'error');

    const { data: arquivos, total_bytes, limite_bytes, espaco_disponivel, arquivo_principal } = res;
    const pct = Math.min(100, (total_bytes / limite_bytes) * 100);
    const spaceClass = pct > 90 ? 'leg-space--danger' : pct > 70 ? 'leg-space--warning' : '';

    // Buscar info da legislacao
    const legRes = await API(`/api/legislacoes/${legId}`);
    const legData = legRes.success ? legRes.data : {};
    const titulo = [legData.tipo_nome, legData.numero, legData.ano].filter(Boolean).join(' ') || `Legislacao #${legId}`;

    // Renderizar modal
    _modalOverlay = document.createElement('div');
    _modalOverlay.className = 'leg-modal-overlay';
    _modalOverlay.innerHTML = `
      <div class="leg-modal">
        <div class="leg-modal-header">
          <h3>${ICONS.files} Arquivos - ${escHtml(titulo)}</h3>
          <button class="leg-modal-close" onclick="LegManager.closeModal()">${ICONS.close}</button>
        </div>
        <div class="leg-modal-body">
          <div class="leg-space-info">
            ${fmtBytes(total_bytes)} de ${fmtBytes(limite_bytes)} utilizados
            (${fmtBytes(espaco_disponivel)} disponiveis)
          </div>
          <div class="leg-space-bar">
            <div class="leg-space-bar-fill ${spaceClass}" style="width:${pct}%"></div>
          </div>

          <ul class="leg-file-list" id="leg-file-list">
            ${arquivos.length === 0
              ? '<li class="leg-empty">Nenhum arquivo associado a esta legislacao</li>'
              : arquivos.map(a => renderFileItem(a, legId)).join('')}
          </ul>

          <div class="leg-upload-zone" id="leg-upload-zone">
            <div class="leg-upload-icon">${ICONS.upload}</div>
            <p><strong>Clique para selecionar</strong> ou arraste arquivos aqui</p>
            <input type="file" id="leg-file-input" multiple accept=".pdf,.doc,.docx,.xls,.xlsx,.csv,.txt,.odt,.rtf,.jpg,.jpeg,.png,.gif">
          </div>
          <div class="leg-upload-limit">Limite total: 100 MB por legislacao. Formatos aceitos: PDF, DOC, DOCX, XLS, XLSX, CSV, TXT, imagens</div>
        </div>
        <div class="leg-modal-footer">
          <span class="leg-space-info" id="leg-space-footer">${arquivos.length} arquivo(s)</span>
          <button class="leg-btn-primary" onclick="LegManager.closeModal()">Fechar</button>
        </div>
      </div>`;

    document.body.appendChild(_modalOverlay);
    requestAnimationFrame(() => _modalOverlay.classList.add('leg-modal--show'));

    // Event listeners
    const zone = document.getElementById('leg-upload-zone');
    const input = document.getElementById('leg-file-input');

    zone.addEventListener('click', () => input.click());
    zone.addEventListener('dragover', (e) => { e.preventDefault(); zone.classList.add('leg-upload--drag'); });
    zone.addEventListener('dragleave', () => zone.classList.remove('leg-upload--drag'));
    zone.addEventListener('drop', (e) => {
      e.preventDefault();
      zone.classList.remove('leg-upload--drag');
      if (e.dataTransfer.files.length) uploadFiles(legId, e.dataTransfer.files);
    });
    input.addEventListener('change', () => {
      if (input.files.length) uploadFiles(legId, input.files);
      input.value = '';
    });

    // Fechar com ESC ou click no overlay
    _modalOverlay.addEventListener('click', (e) => {
      if (e.target === _modalOverlay) closeModal();
    });
    const escHandler = (e) => {
      if (e.key === 'Escape') { closeModal(); document.removeEventListener('keydown', escHandler); }
    };
    document.addEventListener('keydown', escHandler);
  };

  const renderFileItem = (arq, legId) => {
    const tipo = arq.arquivo_tipo || arq.nome_arquivo?.split('.').pop() || '';
    return `
      <li class="leg-file-item" data-arq-id="${arq.id}">
        <div class="leg-file-icon ${getFileIconClass(tipo)}">${getFileIconLabel(tipo)}</div>
        <div class="leg-file-info">
          <div class="leg-file-name" title="${escHtml(arq.nome_arquivo)}">${escHtml(arq.nome_arquivo)}</div>
          <div class="leg-file-meta">${fmtBytes(arq.tamanho_bytes)} &middot; ${fmtDate(arq.criado_em)}</div>
        </div>
        <div class="leg-file-actions">
          <button onclick="LegManager.downloadArquivo(${legId}, ${arq.id})" title="Download">
            ${ICONS.dlFile}
          </button>
          <button class="leg-file-del" onclick="LegManager.deleteFile(${legId}, ${arq.id}, '${escHtml(arq.nome_arquivo)}')" title="Excluir">
            ${ICONS.trash}
          </button>
        </div>
      </li>`;
  };

  const uploadFiles = async (legId, fileList) => {
    const formData = new FormData();
    for (const f of fileList) {
      formData.append('arquivos', f);
    }

    const zone = document.getElementById('leg-upload-zone');
    if (zone) zone.innerHTML = `<div class="leg-spinner"></div><p>Enviando ${fileList.length} arquivo(s)...</p>`;

    try {
      const res = await fetch(`/api/legislacoes/${legId}/arquivos`, { method: 'POST', body: formData });
      const data = await res.json();

      if (data.erros && data.erros.length > 0) {
        data.erros.forEach(e => toast(e, 'error'));
      }
      if (data.arquivos && data.arquivos.length > 0) {
        toast(`${data.arquivos.length} arquivo(s) enviado(s)`);
        // Adicionar a lista sem reabrir o modal
        const list = document.getElementById('leg-file-list');
        if (list) {
          const empty = list.querySelector('.leg-empty');
          if (empty) empty.remove();
          data.arquivos.forEach(a => {
            list.insertAdjacentHTML('beforeend', renderFileItem(a, legId));
          });
        }
        updateSpaceInfo(legId);
        // Atualizar badge na tabela
        document.dispatchEvent(new CustomEvent('legFilesChanged', { detail: { id: legId } }));
        if (typeof carregarLegislacoes === 'function') carregarLegislacoes();
      }
    } catch (err) {
      toast('Erro no upload: ' + (err.message || err), 'error');
    }

    // Restaurar zona de upload
    if (zone) {
      zone.innerHTML = `
        <div class="leg-upload-icon">${ICONS.upload}</div>
        <p><strong>Clique para selecionar</strong> ou arraste arquivos aqui</p>
        <input type="file" id="leg-file-input" multiple accept=".pdf,.doc,.docx,.xls,.xlsx,.csv,.txt,.odt,.rtf,.jpg,.jpeg,.png,.gif">`;
      const input = document.getElementById('leg-file-input');
      zone.onclick = () => input.click();
      input.addEventListener('change', () => {
        if (input.files.length) uploadFiles(legId, input.files);
        input.value = '';
      });
    }
  };

  const deleteFile = async (legId, arqId, nome) => {
    if (!confirm(`Excluir o arquivo "${nome}"?`)) return;

    try {
      const res = await API(`/api/legislacoes/${legId}/arquivos/${arqId}`, { method: 'DELETE' });
      if (!res.success) throw new Error(res.error);
      toast(res.message || 'Arquivo excluido');
      // Remover da lista
      const item = document.querySelector(`.leg-file-item[data-arq-id="${arqId}"]`);
      if (item) {
        item.style.opacity = '0';
        item.style.transform = 'translateX(20px)';
        item.style.transition = 'all .2s';
        setTimeout(() => {
          item.remove();
          const list = document.getElementById('leg-file-list');
          if (list && list.children.length === 0) {
            list.innerHTML = '<li class="leg-empty">Nenhum arquivo associado a esta legislacao</li>';
          }
        }, 200);
      }
      updateSpaceInfo(legId);
      document.dispatchEvent(new CustomEvent('legFilesChanged', { detail: { id: legId } }));
      if (typeof carregarLegislacoes === 'function') carregarLegislacoes();
    } catch (err) {
      toast('Erro ao excluir: ' + (err.message || err), 'error');
    }
  };

  const updateSpaceInfo = async (legId) => {
    try {
      const res = await API(`/api/legislacoes/${legId}/arquivos`);
      if (!res.success) return;
      const { total_bytes, limite_bytes, espaco_disponivel, data: arquivos } = res;
      const pct = Math.min(100, (total_bytes / limite_bytes) * 100);
      const spaceClass = pct > 90 ? 'leg-space--danger' : pct > 70 ? 'leg-space--warning' : '';
      const fill = document.querySelector('.leg-space-bar-fill');
      if (fill) { fill.style.width = pct + '%'; fill.className = 'leg-space-bar-fill ' + spaceClass; }
      const info = document.querySelector('.leg-modal-body .leg-space-info');
      if (info) info.textContent = `${fmtBytes(total_bytes)} de ${fmtBytes(limite_bytes)} utilizados (${fmtBytes(espaco_disponivel)} disponiveis)`;
      const footer = document.getElementById('leg-space-footer');
      if (footer) footer.textContent = `${arquivos.length} arquivo(s)`;
    } catch (e) { /* silent */ }
  };

  const closeModal = () => {
    if (_modalOverlay) {
      _modalOverlay.classList.remove('leg-modal--show');
      setTimeout(() => { _modalOverlay.remove(); _modalOverlay = null; }, 200);
    }
    _modalLegId = null;
  };

  // =====================================================================
  //  INIT
  // =====================================================================
  const init = () => {
    injectCSS();
    console.log('LegManager initialized');
  };

  // =====================================================================
  //  API PUBLICA
  // =====================================================================
  return {
    init,
    renderActions,
    startEdit,
    saveEdit,
    cancelEdit,
    download,
    downloadArquivo,
    openFiles,
    closeModal,
    deleteFile,
  };

})();

