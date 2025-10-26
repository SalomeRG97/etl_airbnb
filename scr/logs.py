from datetime import datetime
import logging
import os

class Logs:
    def __init__(self, file_name='logs'):
        self.fecha_hora = datetime.now().strftime('%Y%m%d_%H%M%S')
        # Crear el directorio logs si no existe
        os.makedirs('logs', exist_ok=True)
        logging.basicConfig(filename=f'logs/{file_name}_{self.fecha_hora}.log', level=logging.INFO,filemode='a',  # Añade 'filemode' con valor 'a' para asegurar el append
                            format='%(asctime)s - %(levelname)s - %(message)s')

    def log(self, mensaje, nivel):
        if nivel == 'info':
            logging.info(mensaje)
        elif nivel == 'error':
            logging.error(mensaje)