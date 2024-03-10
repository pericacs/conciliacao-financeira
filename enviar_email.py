# flake8: noqa
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import os

def enviar_email(destinatario, assunto, mensagem, arquivo_anexo):
    # Configurações do servidor SMTP
    smtp_host = 'smtp.alibin.com.br'
    smtp_port = 587  # Porta do servidor SMTP (normalmente 587 para TLS)

    # Credenciais de login
    email_rem = 'admin@alibin.com.br'
    senha = 'senha@102030'

    # Criar uma mensagem de email
    msg = MIMEMultipart()
    msg['From'] = email_rem
    # msg['To'] = destinatario
    msg['To'] = ', '.join(destinatario)
    msg['Subject'] = assunto

    # Adicionar o corpo da mensagem
    msg.attach(MIMEText(mensagem, 'plain'))

    # Adicionar o arquivo Excel como anexo    
    if arquivo_anexo != '':
        filename = os.path.basename(arquivo_anexo)
        attachment = open(arquivo_anexo, 'rb')  
        part = MIMEBase('application', "octet-stream")
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{filename}"')    
        msg.attach(part)

    # Iniciar uma conexão SMTP
    server = smtplib.SMTP(smtp_host, smtp_port)
    # server.starttls()

    # Fazer login no servidor SMTP
    server.login(email_rem, senha)

    # Enviar o email
    server.sendmail(email_rem, destinatario, msg.as_string())

    # Fechar a conexão SMTP
    server.quit()