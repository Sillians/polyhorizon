import asyncio
import signal
from polyhorizon.ingestion.utils.logger import get_logger, configure_logging
from polyhorizon.ingestion.configs.settings import load_config
from polyhorizon.ingestion.finnhub_producer.producer import FinnhubProducer
from polyhorizon.ingestion.finnhub_consumer.consumer import FinnhubConsumer


async def main():
    config = load_config()
    configure_logging(config.logging)
    logger = get_logger("Real-time stock data streaming")
    logger.info("Start producer and consumer for real-time stock data streaming.")
    logger.info("Starting Polyghorizon real-time data pipeline...")
    
    logger.info("Start consumer in background thread")
    consumer = FinnhubConsumer(settings=config)
    consumer.start(blocking=False)
    
    logger.info("Start producer with WebSocket streaming")
    producer = FinnhubProducer(settings=config)
    
    logger.info("Setup graceful shutdown")
    def shutdown_handler(signum, frame):
        logger.info("Shutting down pipeline...")
        consumer.stop()
        asyncio.create_task(producer.stop())
    
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    
    try:
        await producer.start()
    except KeyboardInterrupt:
        logger.info("Pipeline stopped")
    finally:
        consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
    
