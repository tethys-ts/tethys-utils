FROM python:3.8-slim

# ENV TZ='Pacific/Auckland'

# RUN apt-get update && apt-get install -y unixodbc-dev gcc g++ libspatialindex-dev python-rtree

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY working.py ./

CMD ["python", "-u", "working.py"]
