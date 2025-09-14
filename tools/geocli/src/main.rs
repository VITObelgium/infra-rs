use std::path::PathBuf;

use clap::{Parser, Subcommand, arg};
use geo::{
    Result,
    raster::{self, DenseRaster, RasterReadWrite},
};

#[derive(Parser, Debug)]
#[command(name = "geo toolbox")]
#[command(about = "Geospatial toolbox CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    #[command(name = "cluster-id", about = "Cluster connected regions with the same value")]
    ClusterId {
        #[arg(short = 'i', long = "input", help = "Input file path")]
        input_path: PathBuf,
        #[arg(short = 'o', long = "output", help = "Output file path")]
        output_path: PathBuf,
        #[arg(short = 'd', long = "diagonals", help = "Include diagonal neighbor cells in clustering")]
        diagonals: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::ClusterId {
            input_path,
            output_path,
            diagonals,
        } => {
            let input = DenseRaster::<i64>::read(input_path)?;
            let diagonals = if diagonals {
                raster::algo::ClusterDiagonals::Include
            } else {
                raster::algo::ClusterDiagonals::Exclude
            };

            let output = raster::algo::cluster_id(&input, diagonals);
            output.into_write(output_path)?;
            // indoc::printdoc!(
            //     r#"
            //     Writing config schema to: `{}`
            //     Use this schema in json to validate your config file

            //     Add to your config file the following line:
            //     {{
            //         "$schema": "{}"
            //     }}
            //     "#,
            //     absolute_path.display(),
            //     relative_path.display()
            // );
            // write_config_as_json_schema(output_path)?;
        }
    }

    Ok(())
}
