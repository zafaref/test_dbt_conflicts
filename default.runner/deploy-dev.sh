aws ecr get-login-password --region us-east-1 --profile data-engineering-dev | docker login --username AWS --password-stdin 800110064595.dkr.ecr.us-east-1.amazonaws.com &&
docker pull prefecthq/prefect:2-python3.11 &&
docker build -t prefect/default.runner . &&
docker tag prefect/default.runner:latest 800110064595.dkr.ecr.us-east-1.amazonaws.com/prefect/default.runner:latest &&
docker push 800110064595.dkr.ecr.us-east-1.amazonaws.com/prefect/default.runner:latest
