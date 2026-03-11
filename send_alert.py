"""
send_alert.py
Envia alertas por e-mail quando encontra erros na validação da Camada Bronze.
Usa Gmail com App Password (sem precisar de serviço externo).
"""

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def build_email_body(results, run_time):
    """Monta o corpo HTML do e-mail de alerta."""

    erros_por_pasta = {
        r["pasta"]: r["erros"] + r["avisos"]
        for r in results
        if r["erros"] or r["avisos"]
    }

    if not erros_por_pasta:
        return None, None

    total_erros  = sum(len(r["erros"])  for r in results)
    total_avisos = sum(len(r["avisos"]) for r in results)

    linhas_html = ""
    linhas_txt  = ""

    for pasta, msgs in erros_por_pasta.items():
        nome = pasta.split("/")[-1].upper()
        r_atual = next(r for r in results if r["pasta"] == pasta)

        for msg in r_atual["erros"]:
            linhas_html += f"""
        <tr>
          <td style="padding:10px 14px;font-weight:700;color:#ff4d6a;
                     font-family:monospace;background:#1a0a0e;border-radius:6px">{nome}</td>
          <td style="padding:10px 14px;color:#e8eaf0">❌ {msg}</td>
        </tr>"""

        for msg in r_atual["avisos"]:
            linhas_html += f"""
        <tr>
          <td style="padding:10px 14px;font-weight:700;color:#ffc43d;
                     font-family:monospace;background:#1a0e00;border-radius:6px">{nome}</td>
          <td style="padding:10px 14px;color:#e8eaf0">⚠️ {msg}</td>
        </tr>"""

        linhas_txt += f"\n[{nome}]\n" + "\n".join(f"  • {m}" for m in msgs) + "\n"

    if total_erros > 0 and total_avisos > 0:
        subject = f"❌ Bronze Validation — {total_erros} erro(s) e {total_avisos} aviso(s) [{run_time}]"
    elif total_erros > 0:
        subject = f"❌ Bronze Validation — {total_erros} erro(s) encontrado(s) [{run_time}]"
    else:
        subject = f"⚠️ Bronze Validation — {total_avisos} aviso(s) encontrado(s) [{run_time}]"

    html_body = f"""
    <html><body style="margin:0;padding:0;background:#0a0c10;font-family:'Segoe UI',sans-serif;color:#e8eaf0">
      <div style="max-width:640px;margin:40px auto;background:#111318;border-radius:16px;
                  border:1px solid #1e2330;overflow:hidden">

        <!-- Header -->
        <div style="background:#181c24;padding:28px 32px;border-bottom:1px solid #1e2330">
          <h1 style="margin:0;font-size:22px;font-weight:800;color:#ff4d6a">
            ⚠️ Bronze Layer — Alerta de Validação
          </h1>
          <p style="margin:8px 0 0;color:#5a6070;font-size:13px;font-family:monospace">
            Projeto Revisus · {run_time}
          </p>
        </div>

        <!-- Summary -->
        <div style="padding:24px 32px">
          <p style="font-size:15px;color:#9aa0b0;margin-bottom:20px">
            Foram encontrados <strong style="color:#ff4d6a">{total_erros} erro(s)</strong>
            e <strong style="color:#ffc43d">{total_avisos} aviso(s)</strong>
            na última execução de validação da Camada Bronze.
          </p>

          <table style="width:100%;border-collapse:collapse;font-size:13px">
            <thead>
              <tr style="background:#0a0c10">
                <th style="padding:10px 14px;text-align:left;color:#5a6070;
                           font-family:monospace;font-size:11px;letter-spacing:1px;
                           text-transform:uppercase">Entidade</th>
                <th style="padding:10px 14px;text-align:left;color:#5a6070;
                           font-family:monospace;font-size:11px;letter-spacing:1px;
                           text-transform:uppercase">Problemas</th>
              </tr>
            </thead>
            <tbody>{linhas_html}</tbody>
          </table>
        </div>

        <!-- CTA -->
        <div style="padding:0 32px 28px">
          <a href="https://passosdmar-dev.github.io/Bronze_Validation_Automatico"
             style="display:inline-block;background:#00e5a0;color:#0a0c10;
                    padding:12px 24px;border-radius:8px;font-weight:700;
                    text-decoration:none;font-size:14px">
            Ver Dashboard Completo →
          </a>
        </div>

        <div style="padding:16px 32px;border-top:1px solid #1e2330;
                    font-size:11px;color:#5a6070;font-family:monospace">
          Gerado automaticamente por GitHub Actions · Projeto Revisus Bronze Monitoring
        </div>
      </div>
    </body></html>"""

    return subject, html_body


def send_alert_if_needed(results, run_time):
    """Verifica se há erros e envia e-mail se necessário."""

    tem_erros  = any(r["erros"]  for r in results)
    tem_avisos = any(r["avisos"] for r in results)
    if not tem_erros and not tem_avisos:
        print("✅ Nenhum erro ou aviso — alerta de e-mail não enviado.")
        return

    # Variáveis de ambiente (configuradas nos Secrets do GitHub)
    gmail_user  = os.getenv("ALERT_EMAIL_FROM")   # seu Gmail remetente
    gmail_pass  = os.getenv("ALERT_EMAIL_PASS")   # App Password do Gmail
    email_to    = os.getenv("ALERT_EMAIL_TO")     # destinatário(s), separado por vírgula

    if not all([gmail_user, gmail_pass, email_to]):
        print("⚠️  Variáveis de e-mail não configuradas — pulando alerta.")
        print("   Configure: ALERT_EMAIL_FROM, ALERT_EMAIL_PASS, ALERT_EMAIL_TO")
        return

    subject, html_body = build_email_body(results, run_time)
    if not html_body:
        return

    destinatarios = [e.strip() for e in email_to.split(",")]

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = gmail_user
    msg["To"]      = ", ".join(destinatarios)
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(gmail_user, gmail_pass)
            server.sendmail(gmail_user, destinatarios, msg.as_string())
        print(f"📧 Alerta enviado para: {', '.join(destinatarios)}")
    except Exception as e:
        print(f"❌ Erro ao enviar e-mail: {e}")
