FROM python:3
WORKDIR /app
COPY app/ .
RUN pip install -r requirements.txt
ENV USERNAME ""
ENV PASSWORD ""
CMD ["/bin/bash", "start-app.sh"]