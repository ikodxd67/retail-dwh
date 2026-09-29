FROM python:3.12-slim
RUN pip install --no-cache-dir "confluent-kafka>=2.5" "pyyaml>=6.0"
# код генератора монтируется томом из ./src
CMD ["python", "-m", "retail_dwh.generator.events"]
