FROM python
LABEL authors="fafulja"

COPY api.py /api.py
COPY config.py /config.py
COPY requirements.txt /requirements.txt

RUN pip install -r ./requirements.txt

ENTRYPOINT ["python", "api.py"]