FROM python:2.7.13-alpine
RUN apk update && apk add git  && pip install pip --upgrade
RUN pip install flake8
RUN git clone https://github.com/kiip/bloom-python-driver && cd bloom-python-driver  && python setup.py install
COPY *.py .
CMD python *.py
