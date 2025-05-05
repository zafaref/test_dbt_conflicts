aws ecr get-login-password --region us-east-1 --profile data-engineering-prod | docker login --username AWS --password-stdin 960969968266.dkr.ecr.us-east-1.amazonaws.com &&
docker pull prefecthq/prefect:2-python3.11 &&
docker build -t prefect/default.runner . &&
docker tag prefect/default.runner:latest 960969968266.dkr.ecr.us-east-1.amazonaws.com/prefect/default.runner:latest &&
docker push 960969968266.dkr.ecr.us-east-1.amazonaws.com/prefect/default.runner:latest
