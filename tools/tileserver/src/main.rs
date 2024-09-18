#![warn(clippy::unwrap_used)]
use std::path::PathBuf;

use clap::Parser;
use env_logger::{Env, TimestampPrecision};
use tileserver::tileapihandler;

#[derive(Parser, Debug)]
#[clap(name = "tileserver", about = "The tile server")]
pub struct Opt {
    // set the listen addr
    #[clap(short = 'a', long = "addr")]
    pub addr: Option<String>,

    // set the listen port
    #[clap(short = 'p', long = "port", default_value = "8080")]
    pub port: u16,

    // set the directory where static files are to be found
    #[clap(long = "gis-dir")]
    pub gis_dir: PathBuf,
}

#[tokio::main]
async fn main() {
    use std::{path::PathBuf, str::FromStr};

    let opt = Opt::parse();

    env_logger::Builder::from_env(Env::default().default_filter_or("warn"))
        .format_timestamp(Some(TimestampPrecision::Millis))
        .init();

    let exe_dir = PathBuf::from(
        std::env::current_exe()
            .expect("Unable to get current executable path")
            .parent()
            .expect("Unable to get parent directory of executable"),
    );

    let gdal_config = geo::RuntimeConfiguration::builder().proj_db(&exe_dir).build();
    gdal_config.apply().expect("Failed to configure GDAL");

    let app = tileapihandler::create_router(&opt.gis_dir);

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
