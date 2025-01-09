FROM rust:1.81.0-bookworm

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
    bash-completion \
    flex \
    bison \
    git \
    nodejs

ENV CURL_HOME=/etc
RUN echo "-k" > $CURL_HOME/.curlrc
RUN echo "source /etc/profile.d/bash_completion.sh" >> $HOME/.bashrc

RUN curl -fsSL https://pixi.sh/install.sh | bash
RUN cargo install cargo-binstall && cargo binstall -y just fd-find sd cargo-vcpkg cargo-nextest
RUN just --completions bash > /usr/share/bash-completion/completions/just

CMD ["bash"]
