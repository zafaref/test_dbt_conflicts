FROM prefecthq/prefect:2-python3.11

##############################################
## Microsoft SQL Server ODBC driver for Linux
RUN apt-get update && \
    apt-get install -y gnupg curl freetds-dev libkrb5-dev libssl-dev libgssapi-krb5-2

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 mssql-tools unixodbc-dev g++ && \
    echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc && \
    . ~/.bashrc && \
    apt clean all

# Update OpenSSL configuration file and downgrade security level for sql server compatibility
ADD openssl_append.cnf /tmp/
RUN cat /tmp/openssl_append.cnf >> /etc/ssl/openssl.cnf

#################################
## Enable github cloning via SSH 
RUN apt-get update && \
    apt-get install -y openssh-client && \
    apt clean all

# Add repo build keys and mark read-only (Note: we want this to break if the keys are not present)
ADD .ssh/config /root/.ssh/config
ADD .ssh/de-dbt-build-key /root/.ssh/de-dbt-build-key
ADD .ssh/de-prefect-build-key /root/.ssh/de-prefect-build-key
ADD .ssh/analytics-land-travel-build-key /root/.ssh/analytics-land-travel-build-key
ADD .ssh/next-gen-travel-build-key /root/.ssh/next-gen-travel-build-key
ADD .ssh/mkt-automation-dbt-build-key /root/.ssh/mkt-automation-dbt-build-key
ADD .ssh/bi-toursbi-build-key /root/.ssh/bi-toursbi-build-key
ADD .ssh/bi-toursbistaging-build-key /root/.ssh/bi-toursbistaging-build-key
RUN chmod 400 ~/.ssh/*

# Add all github public keys to known hosts file
RUN ssh-keyscan -t rsa github.com >> ~/.ssh/known_hosts

# Add Oracle TNS and wallet files
RUN mkdir -p /root/oracle/network/admin/{oax8215182500,zsgo89pbja9boffy}/{dev,prod}
# oax8215182500
ADD oracle/network/admin/oax8215182500/dev/tnsnames.ora /root/oracle/network/admin/oax8215182500/dev/tnsnames.ora
ADD oracle/network/admin/oax8215182500/dev/ewallet.pem /root/oracle/network/admin/oax8215182500/dev/ewallet.pem
ADD oracle/network/admin/oax8215182500/prod/tnsnames.ora /root/oracle/network/admin/oax8215182500/prod/tnsnames.ora
ADD oracle/network/admin/oax8215182500/prod/ewallet.pem /root/oracle/network/admin/oax8215182500/prod/ewallet.pem
# zsgo89pbja9boffy
ADD oracle/network/admin/zsgo89pbja9boffy/prod/tnsnames.ora /root/oracle/network/admin/zsgo89pbja9boffy/prod/tnsnames.ora
ADD oracle/network/admin/zsgo89pbja9boffy/prod/ewallet.pem /root/oracle/network/admin/zsgo89pbja9boffy/prod/ewallet.pem

###############################
## Install python dependencies 
ADD ./prefect-ef ./prefect-ef
RUN pip install --upgrade pip
RUN pip install ./prefect-ef

###############################################################
## Disable HTTP2 to prevent agent/cloud communication failures
# RUN prefect config set PREFECT_API_ENABLE_HTTP2=false

## Disable task parameter introspection warning
RUN prefect config set PREFECT_TASK_INTROSPECTION_WARN_THRESHOLD=0

## Enable result persistence and set default result storage block
RUN prefect config set PREFECT_RESULTS_PERSIST_BY_DEFAULT=true
RUN prefect config set PREFECT_DEFAULT_RESULT_STORAGE_BLOCK=s3-bucket/de-prefect
