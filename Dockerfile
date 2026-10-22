# Sketch only. This image has not been built and has not been run.
# python:3.12-slim is a public developer base. An AirflowCluster custom
# resource still refuses any image outside registry.local/astroplane/.
# uvicorn is not a project dependency.
FROM python:3.12-slim

WORKDIR /app
COPY . .
RUN pip install --no-cache-dir .

EXPOSE 8080
CMD ["python", "-m", "uvicorn", "astroplane.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8080"]
