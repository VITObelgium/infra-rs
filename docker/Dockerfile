FROM rust:1.81.0-bookworm

ARG USER_ID
ARG GROUP_ID
RUN groupadd -g ${GROUP_ID} jenkins && useradd --create-home -u ${USER_ID} -g jenkins jenkins

RUN apt update && apt install -y \
    build-essential \
    linux-libc-dev \
    cmake \
    ninja-build \
    pkgconf \
    libpython3-dev \
    python3-pip \
    curl \
    zip \
    unzip \
    tar \
    bash \
    flex \
    bison \
    git \
    nodejs

ENV CURL_HOME=/etc
RUN echo "-k" > $CURL_HOME/.curlrc

CMD ["bash"]
