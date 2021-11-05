FROM jupyter/pyspark-notebook
ADD spark-submit.sh .
USER root
RUN chmod 777 ./spark-submit.sh
ADD consumer.py .
ADD requirements.txt .
RUN python3 -m pip install -r requirements.txt
CMD ["./spark-submit.sh"]
