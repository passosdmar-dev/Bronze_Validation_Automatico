"""
generate_dashboard.py
Roda as validações da Camada Bronze e gera o index.html atualizado automaticamente.
Executado pelo GitHub Actions a cada execução agendada.
"""

import os
import io
import json
import datetime
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from send_alert import send_alert_if_needed

# ── Conexão com o Data Lake ──────────────────────────────────────────────────
ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT")
ACCOUNT_KEY  = os.getenv("AZURE_STORAGE_KEY")
FILE_SYSTEM  = "mvp-connect-express"

def get_client():
    svc = DataLakeServiceClient(
        account_url=f"https://{ACCOUNT_NAME}.dfs.core.windows.net",
        credential=ACCOUNT_KEY
    )
    return svc.get_file_system_client(FILE_SYSTEM)

# ── Schemas esperados ────────────────────────────────────────────────────────
SCHEMAS = {
    "creare/events":          ["event_id","vehicle_id","driver_id","event_type_id","timestamp","inserted_on","is_fix_point"],
    "creare/tracking":        ["tracking_id","vehicle_id","customer_child_id","customer_child_hierarchy_level","vehicle_uuid","driver_id","timestamp","latitude","longitude"],
    "creare/drivers":         ["driver_id","driver_name","integration","license_number","badge_allow_unlock_vehicle","badge_info"],
    "creare/pontos_notaveis": ["ponto_notavel_id","fence_id","fence_description","fence_status","vel_out","status","customer_child_id","updated_at"],
}

REQUIRED_COLS = {
    "creare/events":          ["event_id","vehicle_id","timestamp"],
    "creare/tracking":        ["latitude","longitude"],
    "creare/drivers":         ["driver_id","driver_name"],
    "creare/pontos_notaveis": ["ponto_notavel_id","fence_id"],
}

UNIQUE_KEYS = {
    "creare/events":          "event_id",
    "creare/tracking":        "tracking_id",
    "creare/drivers":         "driver_id",
    "creare/pontos_notaveis": "ponto_notavel_id",
}

VOLUME_BASELINE = {
    "creare/events":          (30, 2000),
    "creare/tracking":        (500, 5000),
    "creare/drivers":         (100, 5000),
    "creare/pontos_notaveis": (10, 1000),
}

ICONS = {
    "creare/events":          ("📡", "#00e5a0"),
    "creare/tracking":        ("📍", "#4da6ff"),
    "creare/drivers":         ("🧑‍✈️", "#ffc43d"),
    "creare/pontos_notaveis": ("📌", "#ff4d6a"),
}

# ── Validação ────────────────────────────────────────────────────────────────
def validate_entity(fs, pasta):
    result = {
        "pasta": pasta,
        "arquivo": None,
        "ultima_mod": None,
        "linhas": 0,
        "colunas": 0,
        "total_arquivos": 0,
        "arquivos_vazios": 0,
        "schema_ok": False,
        "colunas_faltando": [],
        "nulos": {},
        "duplicados": 0,
        "volume_ok": False,
        "volume_min": 0,
        "volume_max": 0,
        "erros": [],
        "avisos": [],
    }

    try:
        paths = list(fs.get_paths(path=pasta))
        result["total_arquivos"] = len(paths)
        result["arquivos_vazios"] = sum(1 for p in paths if p.content_length == 0)

        validos = [p for p in paths if p.content_length and p.content_length > 0]
        if not validos:
            result["erros"].append("Nenhum arquivo válido encontrado")
            return result

        ultimo = max(validos, key=lambda x: x.last_modified)
        result["arquivo"]    = ultimo.name
        result["ultima_mod"] = str(ultimo.last_modified)

        fc   = fs.get_file_client(ultimo.name)
        data = fc.download_file().readall()
        df   = pd.read_parquet(io.BytesIO(data))

        result["linhas"]   = len(df)
        result["colunas"]  = len(df.columns)

        # Schema
        esperado = set(SCHEMAS.get(pasta, []))
        atual    = set(df.columns)
        faltando = list(esperado - atual)
        result["colunas_faltando"] = faltando
        result["schema_ok"]        = len(faltando) == 0
        if faltando:
            result["erros"].append(f"Colunas faltando: {faltando}")

        # Nulos
        for col in REQUIRED_COLS.get(pasta, []):
            if col in df.columns:
                n = int(df[col].isnull().sum())
                if n > 0:
                    result["nulos"][col] = n
                    result["erros"].append(f"'{col}' tem {n} valor(es) nulo(s)")

        # Duplicados
        uk = UNIQUE_KEYS.get(pasta)
        if uk and uk in df.columns:
            dup = int(df.duplicated(subset=[uk]).sum())
            result["duplicados"] = dup
            if dup > 0:
                result["erros"].append(f"{dup} duplicado(s) em '{uk}'")

        # Volume
        vmin, vmax = VOLUME_BASELINE.get(pasta, (0, 999999))
        result["volume_min"] = vmin
        result["volume_max"] = vmax
        result["volume_ok"]  = vmin <= len(df) <= vmax
        if not result["volume_ok"]:
            result["avisos"].append(f"Volume {len(df)} fora da faixa esperada ({vmin}–{vmax})")

    except Exception as e:
        result["erros"].append(f"Erro inesperado: {str(e)}")

    return result

# ── Geração do HTML ──────────────────────────────────────────────────────────
def status_pill(erros, avisos):
    if erros:   return '<span class="status-pill error">ERRO</span>'
    if avisos:  return '<span class="status-pill warn">ATENÇÃO</span>'
    return '<span class="status-pill ok">OK</span>'

def check_row(ok, label, value, kind="ok"):
    icon  = "✓" if ok else ("⚠" if kind == "warn" else "✗")
    cls_i = "icon-ok" if ok else ("icon-warn" if kind == "warn" else "icon-err")
    cls_v = "ok" if ok else ("warn" if kind == "warn" else "err")
    return f"""
        <div class="validation-row">
          <span class="check-label"><span class="{cls_i}">{icon}</span> {label}</span>
          <span class="val-result {cls_v}">{value}</span>
        </div>"""

def build_entity_card(r):
    pasta = r["pasta"]
    nome  = pasta.split("/")[-1]
    icon, color = ICONS.get(pasta, ("📦", "#fff"))
    pill  = status_pill(r["erros"], r["avisos"])

    arquivo_curto = (r["arquivo"] or "—").split("/")[-1]
    arquivo_full  = r["arquivo"] or "—"
    ultima_mod    = r["ultima_mod"] or "—"

    rows = ""
    rows += check_row(True, "Arquivo chegou no Data Lake", "OK")
    rows += check_row(r["schema_ok"],
                      f"Schema válido ({r['colunas']} colunas)" if r["schema_ok"]
                      else f"Colunas faltando: {r['colunas_faltando']}",
                      "SCHEMA OK" if r["schema_ok"] else "INVÁLIDO")

    for col, n in r["nulos"].items():
        rows += check_row(False, f"'{col}' — {n} valor(es) nulo(s)", f"{n} NULO(S)")

    for col in REQUIRED_COLS.get(pasta, []):
        if col not in r["nulos"]:
            rows += check_row(True, f"'{col}' — sem valores nulos", "OK")

    dup = r["duplicados"]
    uk  = UNIQUE_KEYS.get(pasta, "id")
    rows += check_row(dup == 0,
                      f"Nenhum duplicado em '{uk}'" if dup == 0 else f"{dup} duplicado(s) em '{uk}'",
                      f"{dup} DUPL" if dup > 0 else "0 DUPL",
                      kind="ok" if dup == 0 else "err")

    rows += check_row(r["volume_ok"],
                      f"Volume: {r['linhas']} regs (esperado {r['volume_min']}–{r['volume_max']})",
                      "OK" if r["volume_ok"] else "FORA DA FAIXA",
                      kind="ok" if r["volume_ok"] else "warn")

    pct_vazio = (r["arquivos_vazios"] / r["total_arquivos"] * 100) if r["total_arquivos"] > 0 else 0
    pct_fill  = 100 - pct_vazio

    return f"""
    <div class="entity-card">
      <div class="entity-header">
        <div>
          <div class="entity-name">
            <div class="entity-icon" style="background:rgba(255,255,255,0.06);color:{color}">{icon}</div>
            creare/{nome}
          </div>
          <div class="entity-path">{arquivo_full}</div>
        </div>
        {pill}
      </div>
      <div class="entity-body">
        <div class="stats-row">
          <div class="stat-box">
            <div class="sval">{r['linhas']}</div><div class="slabel">Linhas</div>
          </div>
          <div class="stat-box">
            <div class="sval">{r['colunas']}</div><div class="slabel">Colunas</div>
          </div>
          <div class="stat-box">
            <div class="sval" style="color:var(--green)">{r['total_arquivos']}</div>
            <div class="slabel">Total Arqs</div>
          </div>
        </div>
        <div class="file-path-box">
          <span class="file-path-label">último arquivo · {ultima_mod}</span>
          {arquivo_full}
        </div>
        <div class="validation-list">{rows}</div>
      </div>
    </div>"""

def build_html(results, run_time, total_validos, total_vazios):
    cards = "\n".join(build_entity_card(r) for r in results)
    total_erros  = sum(len(r["erros"])  for r in results)
    total_avisos = sum(len(r["avisos"]) for r in results)
    schemas_ok   = sum(1 for r in results if r["schema_ok"])
    total_dupl   = sum(r["duplicados"] for r in results)

    global_status = "✅ TUDO OK" if total_erros == 0 else f"⚠️ {total_erros} ERRO(S) ENCONTRADO(S)"
    global_color  = "#00e5a0" if total_erros == 0 else "#ff4d6a"

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta http-equiv="refresh" content="300">
<title>Bronze Layer Validation — Revisus</title>
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;700;800&display=swap" rel="stylesheet">
<style>
:root {{
  --bg:#0a0c10;--surface:#111318;--surface2:#181c24;--border:#1e2330;
  --green:#00e5a0;--green-dim:rgba(0,229,160,.12);--red:#ff4d6a;
  --red-dim:rgba(255,77,106,.12);--yellow:#ffc43d;--yellow-dim:rgba(255,196,61,.12);
  --blue:#4da6ff;--blue-dim:rgba(77,166,255,.1);--text:#e8eaf0;--muted:#5a6070;
}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:var(--bg);color:var(--text);font-family:'Syne',sans-serif;min-height:100vh}}
body::before{{content:'';position:fixed;inset:0;background-image:linear-gradient(rgba(0,229,160,.03) 1px,transparent 1px),linear-gradient(90deg,rgba(0,229,160,.03) 1px,transparent 1px);background-size:40px 40px;pointer-events:none;z-index:0}}
.container{{max-width:1200px;margin:0 auto;padding:40px 24px 80px;position:relative;z-index:1}}
header{{display:flex;align-items:flex-start;justify-content:space-between;gap:24px;margin-bottom:32px;flex-wrap:wrap}}
.header-left h1{{font-size:clamp(26px,4vw,38px);font-weight:800;letter-spacing:-1px;background:linear-gradient(135deg,#fff 40%,var(--green));-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text}}
.header-left p{{margin-top:6px;color:var(--muted);font-family:'Space Mono',monospace;font-size:11px;letter-spacing:1px}}
.header-badge{{display:flex;align-items:center;gap:8px;background:var(--green-dim);border:1px solid rgba(0,229,160,.25);border-radius:999px;padding:8px 18px;font-size:13px;font-weight:600;color:var(--green)}}
.pulse{{width:8px;height:8px;background:var(--green);border-radius:50%;animation:pulse 2s infinite}}
@keyframes pulse{{0%,100%{{opacity:1;transform:scale(1);box-shadow:0 0 0 0 rgba(0,229,160,.4)}}50%{{opacity:.7;transform:scale(1.1);box-shadow:0 0 0 6px rgba(0,229,160,0)}}}}
.timestamp-bar{{display:flex;align-items:center;gap:16px;font-family:'Space Mono',monospace;font-size:11px;color:var(--muted);margin-bottom:32px;padding-bottom:20px;border-bottom:1px solid var(--border);flex-wrap:wrap}}
.timestamp-bar span{{color:var(--green)}}
.global-status{{text-align:center;padding:18px;background:var(--surface);border:1px solid {global_color}33;border-radius:14px;font-size:18px;font-weight:800;color:{global_color};letter-spacing:.5px;margin-bottom:32px}}
.summary-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:14px;margin-bottom:36px}}
.summary-card{{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:22px 18px;position:relative;overflow:hidden;transition:transform .2s}}
.summary-card:hover{{transform:translateY(-3px)}}
.summary-card::after{{content:'';position:absolute;top:0;left:0;right:0;height:3px;border-radius:14px 14px 0 0}}
.summary-card.green::after{{background:var(--green)}}.summary-card.red::after{{background:var(--red)}}.summary-card.yellow::after{{background:var(--yellow)}}.summary-card.blue::after{{background:var(--blue)}}
.summary-card .label{{font-size:10px;font-family:'Space Mono',monospace;color:var(--muted);letter-spacing:1px;text-transform:uppercase;margin-bottom:10px}}
.summary-card .value{{font-size:34px;font-weight:800;letter-spacing:-1px}}
.summary-card.green .value{{color:var(--green)}}.summary-card.red .value{{color:var(--red)}}.summary-card.yellow .value{{color:var(--yellow)}}.summary-card.blue .value{{color:var(--blue)}}
.summary-card .sub{{font-size:11px;color:var(--muted);margin-top:5px}}
.section-title{{font-size:12px;font-family:'Space Mono',monospace;color:var(--muted);letter-spacing:2px;text-transform:uppercase;margin-bottom:18px;padding-left:12px;border-left:3px solid var(--green)}}
.entities-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(520px,1fr));gap:18px;margin-bottom:36px}}
@media(max-width:600px){{.entities-grid{{grid-template-columns:1fr}}}}
.entity-card{{background:var(--surface);border:1px solid var(--border);border-radius:16px;overflow:hidden}}
.entity-header{{padding:16px 22px;background:var(--surface2);border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;gap:12px}}
.entity-name{{font-size:14px;font-weight:700;display:flex;align-items:center;gap:10px}}
.entity-icon{{width:30px;height:30px;border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:14px}}
.entity-path{{font-family:'Space Mono',monospace;font-size:9px;color:var(--muted);margin-top:2px;word-break:break-all}}
.status-pill{{padding:4px 10px;border-radius:999px;font-size:10px;font-weight:700;font-family:'Space Mono',monospace}}
.status-pill.ok{{background:var(--green-dim);color:var(--green);border:1px solid rgba(0,229,160,.25)}}
.status-pill.warn{{background:var(--yellow-dim);color:var(--yellow);border:1px solid rgba(255,196,61,.25)}}
.status-pill.error{{background:var(--red-dim);color:var(--red);border:1px solid rgba(255,77,106,.25)}}
.entity-body{{padding:18px 22px}}
.stats-row{{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:14px}}
.stat-box{{background:var(--bg);border:1px solid var(--border);border-radius:10px;padding:10px;text-align:center}}
.stat-box .sval{{font-size:18px;font-weight:800;letter-spacing:-.5px}}
.stat-box .slabel{{font-size:9px;font-family:'Space Mono',monospace;color:var(--muted);margin-top:2px;text-transform:uppercase}}
.file-path-box{{background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:10px 12px;font-family:'Space Mono',monospace;font-size:9px;color:var(--blue);word-break:break-all;margin-bottom:12px;line-height:1.5}}
.file-path-label{{color:var(--muted);font-size:8px;letter-spacing:1px;text-transform:uppercase;display:block;margin-bottom:3px}}
.validation-list{{display:flex;flex-direction:column;gap:8px}}
.validation-row{{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:9px 12px;border-radius:8px;background:var(--bg);border:1px solid var(--border)}}
.validation-row .check-label{{font-size:12px;color:#9aa0b0;display:flex;align-items:center;gap:7px;flex:1}}
.icon-ok{{color:var(--green);font-size:14px}}.icon-err{{color:var(--red);font-size:14px}}.icon-warn{{color:var(--yellow);font-size:14px}}
.val-result{{font-family:'Space Mono',monospace;font-size:10px;font-weight:700;padding:3px 8px;border-radius:5px;white-space:nowrap}}
.val-result.ok{{background:var(--green-dim);color:var(--green)}}.val-result.err{{background:var(--red-dim);color:var(--red)}}.val-result.warn{{background:var(--yellow-dim);color:var(--yellow)}}
footer{{text-align:center;font-family:'Space Mono',monospace;font-size:10px;color:var(--muted);padding-top:20px;border-top:1px solid var(--border)}}
footer span{{color:var(--green)}}
</style>
</head>
<body>
<div class="container">
  <header>
    <div class="header-left">
      <h1>Bronze Layer<br>Validation Report</h1>
      <p>PROJETO REVISUS · DATA LAKE CREARE · CAMADA BRONZE</p>
    </div>
    <div class="header-badge"><div class="pulse"></div>AUTO-ATUALIZADO</div>
  </header>

  <div class="timestamp-bar">
    <div>🕐 Última execução: <span>{run_time}</span></div>
    <div>📦 Container: <span>{FILE_SYSTEM}</span></div>
    <div>☁️ Account: <span>{ACCOUNT_NAME}</span></div>
    <div>🔄 Atualização automática: <span>a cada 5 min</span></div>
  </div>

  <div class="global-status">{global_status}</div>

  <div class="summary-grid">
    <div class="summary-card green">
      <div class="label">Arquivos Válidos</div>
      <div class="value">{total_validos}</div>
      <div class="sub">de {total_validos + total_vazios} verificados</div>
    </div>
    <div class="summary-card red">
      <div class="label">Arquivos Vazios</div>
      <div class="value">{total_vazios}</div>
      <div class="sub">normal em near-RT</div>
    </div>
    <div class="summary-card green">
      <div class="label">Schema OK</div>
      <div class="value">{schemas_ok}/4</div>
      <div class="sub">entidades validadas</div>
    </div>
    <div class="summary-card {'red' if total_dupl > 0 else 'green'}">
      <div class="label">Duplicados</div>
      <div class="value">{total_dupl}</div>
      <div class="sub">total encontrado</div>
    </div>
    <div class="summary-card {'red' if total_erros > 0 else 'green'}">
      <div class="label">Total Erros</div>
      <div class="value">{total_erros}</div>
      <div class="sub">{total_avisos} aviso(s)</div>
    </div>
  </div>

  <div class="section-title">Validação por Entidade</div>
  <div class="entities-grid">
    {cards}
  </div>

  <footer>
    Projeto Revisus · Bronze Validation · Auto-gerado em <span>{run_time}</span> · GitHub Actions
  </footer>
</div>
</body>
</html>"""

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("🔍 Iniciando validação da Camada Bronze...")
    fs      = get_client()
    results = []

    for pasta in SCHEMAS.keys():
        print(f"   Validando: {pasta}")
        r = validate_entity(fs, pasta)
        results.append(r)
        if r["erros"]:
            print(f"   ❌ Erros: {r['erros']}")
        elif r["avisos"]:
            print(f"   ⚠️  Avisos: {r['avisos']}")
        else:
            print(f"   ✅ OK")

    total_validos = sum(r["total_arquivos"] - r["arquivos_vazios"] for r in results)
    total_vazios  = sum(r["arquivos_vazios"] for r in results)
    run_time      = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Gera dashboard
    html = build_html(results, run_time, total_validos, total_vazios)
    with open("index.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("✅ index.html gerado com sucesso")

    # Salva JSON com resultados (útil para histórico)
    with open("last_run.json", "w", encoding="utf-8") as f:
        json.dump({"run_time": run_time, "results": results}, f, default=str, indent=2)

    # Dispara alertas se necessário
    send_alert_if_needed(results, run_time)

if __name__ == "__main__":
    main()
