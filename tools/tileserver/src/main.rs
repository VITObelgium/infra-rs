#![warn(clippy::unwrap_used)]
use clap::Parser;
use env_logger::{Env, TimestampPrecision};
use tileserver::tileapihandler;

#[tokio::main(worker_threads = 16)]
async fn main() {
    use std::{path::PathBuf, str::FromStr};

    let opt = tileapihandler::Opt::parse();

    env_logger::Builder::from_env(Env::default().default_filter_or("warn"))
        .format_timestamp(Some(TimestampPrecision::Millis))
        .init();

    let exe_dir = PathBuf::from(
        std::env::current_exe()
            .expect("Unable to get current executable path")
            .parent()
            .expect("Unable to get parent directory of executable"),
    );

    let gdal_config = geo::RuntimeConfiguration::new(&exe_dir);

    gdal_config.apply().expect("Failed to configure GDAL");

    let app = tileapihandler::create_router(&opt);

    let ip_addr = match opt.addr {
        Some(addr) => std::net::IpAddr::from_str(addr.as_str()).expect("Invalid ip address provided"),
        None => std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
    };

    let sock_addr = std::net::SocketAddr::from((ip_addr, opt.port));
    log::debug!("Run server on: {sock_addr}");

    let listener = tokio::net::TcpListener::bind(&sock_addr)
        .await
        .expect("Unable to bind to address");
    axum::serve(listener, app).await.expect("Unable to start server");
}
