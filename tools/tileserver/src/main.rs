#![warn(clippy::unwrap_used)]
use std::path::PathBuf;

use clap::Parser;
use env_logger::{Env, TimestampPrecision};
use tiler::Result;
use tileserver::tileapihandler;

#[derive(Parser, Debug)]
#[clap(name = "tileserver", about = "The tile server")]
pub struct Opt {
    // set the listen addr
    #[clap(short = 'a', long = "addr")]
    pub addr: Option<String>,

    // set the listen port
    #[clap(short = 'p', long = "port", default_value = "4444")]
    pub port: u16,

    // set the directory where static files are to be found
    #[clap(long = "gis-dir")]
    pub gis_dir: PathBuf,

    // start in terminal ui mode
    #[cfg(feature = "tui")]
    #[clap(long = "tui")]
    pub tui: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    use std::{path::PathBuf, str::FromStr};

    let opt = Opt::parse();

    if !opt.tui {
        env_logger::Builder::from_env(Env::default().default_filter_or("warn"))
            .format_timestamp(Some(TimestampPrecision::Millis))
            .init();
    } else {
        #[cfg(feature = "tui")]
        tui_logger::init_logger(log::LevelFilter::Info).expect("Failed to initialize logger");
    }

    let exe_dir = PathBuf::from(
        std::env::current_exe()
            .expect("Unable to get current executable path")
            .parent()
            .expect("Unable to get parent directory of executable"),
    );

    let gdal_config = geo::RuntimeConfiguration::builder().proj_db(&exe_dir).build();
    gdal_config.apply().expect("Failed to configure GDAL");

    let ip_addr = match opt.addr {
        Some(addr) => std::net::IpAddr::from_str(addr.as_str()).expect("Invalid ip address provided"),
        None => std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
    };

    let sock_addr = std::net::SocketAddr::from((ip_addr, opt.port));
    let (router, status_rx) = tileapihandler::create_router(&opt.gis_dir);
    let listener = tokio::net::TcpListener::bind(&sock_addr)
        .await
        .expect("Unable to bind to address");

    #[cfg(feature = "tui")]
    if opt.tui {
        tileserver::tui::launch(router, listener, status_rx).await?;
        return Ok(());
    }

    axum::serve(listener, router).await?;
    Ok(())
}
