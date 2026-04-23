_fila_pausada = False

import threading, time, uuid, psycopg2, psycopg2.extras

def iniciar_worker(app, get_db, buscador_jobs):
    def worker():
        time.sleep(30)  # Aguardar Gunicorn estar totalmente pronto
        while True:
            try:
                conn = get_db()
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute("UPDATE fila_buscas SET status='aguardando',job_id=NULL,iniciado_em=NULL WHERE status='rodando' AND (concluido_em IS NULL OR concluido_em > NOW() - INTERVAL '1 minute')")
                conn.commit()
                cur.execute("SELECT * FROM fila_buscas WHERE status='aguardando' ORDER BY ordem ASC,criado_em ASC LIMIT 1")
                item = cur.fetchone()
                cur.close(); conn.close()
                # Verificar pausa
                if _fila_pausada:
                    _ftime.sleep(3)
                    continue
                if not item:
                    time.sleep(3)
                    continue
                job_id = str(uuid.uuid4())[:8]
                c2=get_db(); cu2=c2.cursor()
                cu2.execute("UPDATE fila_buscas SET status='rodando',job_id=%s,iniciado_em=NOW() WHERE id=%s",(job_id,item['id']))
                c2.commit(); cu2.close(); c2.close()
                from modulos.log_persistente import LogList
                buscador_jobs[job_id]={'logs':LogList(job_id,get_db),'result':None,'done':False,'tipo':'auto_fila','ts':time.time(),'municipio':item['municipio'],'estado':item['estado']}
                # INSERT inicial no historico para ter hist_id disponivel durante polling
                try:
                    _ci=get_db(); _cui=_ci.cursor()
                    _cui.execute("INSERT INTO buscas_historico (tipo,municipio,estado,iniciado_em,sucesso,log_texto,pdf_path,anexos_paths,job_id) VALUES (%s,%s,%s,NOW(),%s,%s,%s,%s,%s) RETURNING id",('auto',item['municipio'],item['estado'],False,'','','[]',job_id))
                    _hrow=_cui.fetchone()
                    if _hrow: buscador_jobs[job_id]['hist_id']=_hrow[0]
                    _ci.commit(); _cui.close(); _ci.close()
                except: pass
                try:
                    with app.app_context():
                        from modulos.buscador_urbanistico import buscar_legislacoes_urbanisticas
                        from modulos.buscador_legislacoes import _chamar_llm as _llm_fila
                        def llm(p,l,lb='',mr=2): return _llm_fila(p,l,lb,mr)
                        fb=None
                        try:
                            c3=get_db();cu3=c3.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                            cu3.execute("SELECT url FROM municipio_fallback WHERE LOWER(municipio)=LOWER(%s) AND LOWER(estado)=LOWER(%s)",(item['municipio'],item['estado']))
                            row=cu3.fetchone(); cu3.close(); c3.close()
                            if row: fb=row['url']
                        except: pass
                        r=buscar_legislacoes_urbanisticas(item['municipio'],item['estado'],buscador_jobs[job_id]['logs'],llm,fallback_url=fb,max_legislacoes=item.get('max_legislacoes'))
                        buscador_jobs[job_id]['result']={'encontradas':r.get('encontradas',[]),'zip_url':r.get('zip_url'),'zip_nome':r.get('zip_nome'),'relatorio_url':r.get('relatorio_url'),'relatorio_nome':r.get('relatorio_nome'),'tabela_url':r.get('tabela_url'),'tabela_nome':r.get('tabela_nome'),'custo_usd':r.get('custo_usd'),'nao_encontrada':r.get('nao_encontrada',False),'legislacoes_json':r.get('legislacoes_json',[])}
                        try:
                            enc=r.get('encontradas',[]); leg=enc[0] if enc else {}
                            c4=get_db(); cu4=c4.cursor()
                            cu4.execute("UPDATE buscas_historico SET concluido_em=NOW(),sucesso=%s,legislacao_tipo=%s,legislacao_numero=%s,legislacao_ano=%s,legislacao_link=%s,log_texto=%s,pdf_path=%s,relatorio_path=%s,tabela_path=%s,zip_path=%s WHERE job_id=%s RETURNING id",(bool(enc),leg.get('tipo',''),leg.get('numero',''),leg.get('ano',''),leg.get('link',''),'\n'.join(l.get('msg','') for l in buscador_jobs[job_id]['logs']),leg.get('pdf_path',''),r.get('relatorio_url',''),r.get('tabela_url',''),r.get('zip_url',''),job_id))
                            hist_id_row = cu4.fetchone()
                            if hist_id_row: buscador_jobs[job_id]['hist_id'] = hist_id_row[0]
                            c4.commit(); cu4.close(); c4.close()
                        except: pass
                        c5=get_db(); cu5=c5.cursor()
                        # Adicionar ao dossie_municipios
                        try:
                            c_d=get_db(); cu_d=c_d.cursor()
                            cu_d.execute("INSERT INTO dossie_municipios (municipio, estado, origem) VALUES (%s,%s,%s) ON CONFLICT (municipio, estado) DO NOTHING",(item['municipio'],item['estado'],item.get('origem','manual')))
                            c_d.commit(); cu_d.close(); c_d.close()
                        except: pass
                        cu5.execute("UPDATE fila_buscas SET status='concluido',concluido_em=NOW() WHERE id=%s",(item['id'],))
                        c5.commit(); cu5.close(); c5.close()
                except Exception as eb:
                    try:
                        c6=get_db(); cu6=c6.cursor()
                        cu6.execute("UPDATE fila_buscas SET status='erro',erro=%s,concluido_em=NOW() WHERE id=%s",(str(eb)[:500],item['id']))
                        c6.commit(); cu6.close(); c6.close()
                    except: pass
                finally:
                    buscador_jobs[job_id]['done']=True
                    try:
                        import json
                        with open(f'/var/www/urbanlex/static/downloads/job_{job_id}.jsonl','a') as f:
                            f.write(json.dumps({'_result':buscador_jobs[job_id].get('result',{})})+'\\n')
                    except: pass
            except Exception as _ew_outer:
                import traceback; print(f"[FILA WORKER ERROR] {_ew_outer} {traceback.format_exc()[-300:]}", flush=True); time.sleep(5)
    threading.Thread(target=worker,daemon=True,name='fila-buscas').start()
