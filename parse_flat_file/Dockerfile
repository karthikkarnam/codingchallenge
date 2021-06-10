FROM python:3
WORKDIR /usr/src/app
copy ./*.py .
copy ./*.json .
copy ./*.txt .
ENTRYPOINT ["python", "parse_flat_file.py", "spec.json", "sample_wheader.txt", "op.csv"]
