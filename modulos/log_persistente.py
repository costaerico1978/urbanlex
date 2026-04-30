import psycopg2
import psycopg2.extras

class LogList(list):
    """Lista de logs que persiste automaticamente no banco ao fazer append."""
    def __init__(self, job_id, get_db, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_id = job_id
        self.get_db = get_db

    def append(self, item):
        from datetime import datetime as _dt, timezone as _tz, timedelta as _td
        if isinstance(item, dict) and 'ts' not in item:
            _utc3 = _dt.now(_tz.utc) - _td(hours=3)
            item['ts'] = _utc3.strftime('%H:%M:%S')
        super().append(item)
        # Persistir no banco em background
        try:
            cursor = len(self) - 1
            conn = self.get_db()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO buscas_logs (job_id, cursor, nivel, msg, ts) VALUES (%s,%s,%s,%s,%s)",
                (self.job_id, cursor, item.get('nivel',''), item.get('msg',''), item.get('ts',''))
            )
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            pass  # Nunca travar a busca por falha de persistencia


def carregar_logs(job_id, get_db, cursor_from=0):
    """Carregar logs do banco a partir de um cursor."""
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT cursor, nivel, msg, ts FROM buscas_logs WHERE job_id=%s AND cursor>=%s ORDER BY cursor ASC",
            (job_id, cursor_from)
        )
        logs = [dict(r) for r in cur.fetchall()]
        cur.close()
        conn.close()
        return logs
    except:
        return []


def contar_logs(job_id, get_db):
    """Contar total de logs no banco."""
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM buscas_logs WHERE job_id=%s", (job_id,))
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count
    except:
        return 0
