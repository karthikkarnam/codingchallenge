FROM python:3
WORKDIR /usr/src/app
copy ./*.py .
copy ./*.json .
copy ./*.txt .
CMD ["parse_flat_file.py" "./spec.json" "./smaple_wheader.txt" "./op.csv"]