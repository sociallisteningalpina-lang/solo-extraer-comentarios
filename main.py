# main.py
import extraer_comentarios
import logging

# Configurar logging para ver los mensajes del script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def main():
    """
    Script principal que ejecuta el proceso de extracción de comentarios.
    """
    logging.info("🤖 INICIANDO PROCESO DE EXTRACCIÓN DE COMENTARIOS...")

    try:
        # Ejecutar la extracción de comentarios
        extraer_comentarios.run_extraction()

        logging.info("🎉 ¡EXTRACCIÓN FINALIZADA CON ÉXITO!")

    except Exception as e:
        logging.error("❌ ERROR FATAL: La extracción falló.", exc_info=True)

if __name__ == "__main__":
    main()
