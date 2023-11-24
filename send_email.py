import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_email_smtp(destinatario, asunto, mensaje_htm, count_rowl):
    # Configurar los parámetros del servidor SMTP de Office 365
    servidor_smtp = 'smtp.office365.com'
    puerto_smtp = 587
    usuario = 'servicio.cliente1@cvn.com.co'
    contraseña = 'S3rv1c10.2020*'

    with open("utils\html_specializes.html", 'r', encoding='utf-8') as myfile:
        body = myfile.read()
        body = body.replace('{{numero_registros}}', count_row)
    # Crear el objeto para el servidor SMTP
    server = smtplib.SMTP(servidor_smtp, puerto_smtp)

    # Habilitar el modo de conexión segura (TLS)
    server.starttls()

    try:
        # Iniciar sesión con las credenciales del remitente
        server.login(usuario, contraseña)

        # Crear el mensaje MIMEMultipart con formato HTML
        msg = MIMEMultipart("alternative")
        msg['From'] = usuario
        msg['To'] = destinatario
        msg['Subject'] = asunto

        # Agregar ambas versiones (texto y HTML) al mensaje MIMEMultipart
        
        msg.attach(MIMEText(mensaje_html, 'html'))

        # Enviar el correo electrónico
        server.sendmail(usuario, destinatario, msg.as_string())

        print("El correo electrónico ha sido enviado con éxito.")

    except Exception as e:
        print("Error al enviar el correo electrónico:", e)

    finally:
        # Cerrar la conexión con el servidor SMTP
        server.quit()

