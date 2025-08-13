FROM rust:1.84.0-trixie

ARG USER_ID
ARG GROUP_ID
RUN groupadd -g ${GROUP_ID} jenkins && useradd --create-home -u ${USER_ID} -g jenkins jenkins

RUN apt update && apt install -y \
    build-essential \
    linux-libc-dev \
    cmake \
    ninja-build \
    pkgconf \
    curl \
    zip \
    unzip \
    tar \
    bash \
    flex \
    bison \
    git \
    nodejs \
    sudo

ENV CURL_HOME=/etc
RUN echo "-k" > $CURL_HOME/.curlrc

CMD ["bash"]
