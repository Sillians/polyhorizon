FROM prefecthq/prefect:3-python3.11

# Docker work pools are provided by the prefect-docker integration, not by the
# core Prefect image. Pin the integration to a compatible major/minor range so
# worker capabilities do not change unexpectedly on rebuild.
RUN pip install --no-cache-dir "prefect-docker>=0.6,<0.7"
