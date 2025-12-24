use std::fs::{File};
use std::time::Instant;
use std::collections::HashMap;
use std::collections::BTreeSet;
use std::io::{BufRead, BufReader};

fn main() {
    read_file_with_buffer().expect("Failed to read file");
}

fn read_file_with_buffer() -> std::io::Result<()> {
    let start = Instant::now();

    // Specify file, and reader that'll speed up the reading process
    let f = File::open("./measurements.txt")?;
    let mut reader = BufReader::new(f);

    // StationName --> (min, mean, max, occur)
    let mut stations = HashMap::new();

    // List for sorting later
    let mut sorted_stations: BTreeSet<String> = BTreeSet::new();

    // Variables
    let mut station_name: &str;
    let mut temp: f64;

    // String buffer we can re-use
    let mut line = String::new();

    // let mut x = 0;

    // For each line, extract data and put it into a map
    loop {
        // if x > 100000 {
        //     break;
        // }
        // x += 1;

        match reader.read_line(&mut line) {
            Ok(0) => {
                println!("Reached end of file!");
                break;
            }
            Ok(_) => {
                let mut parts = line.split(';');
                station_name = parts.next().unwrap();
                temp = parts.next().unwrap().trim().parse::<f64>().unwrap();

                sorted_stations.insert(station_name.to_string());
                
                
                let station_info = stations.entry(String::from(station_name)).or_insert((temp, temp, temp, 1.0));

                // Update Min
                if station_info.0 > temp {
                    station_info.0 = temp;
                }

                // Update Total
                station_info.1 += temp;
                station_info.3 += 1.0;

                // Update Max
                if station_info.2 < temp {
                    station_info.2 = temp;
                }
                
                line.clear();
            }
            Err(err) => {
                println!("Error occurred: {}", err);
                break;
            }
        }

    }

    for key in &sorted_stations {
        let station_info = stations[key];

        let mean = station_info.1 / station_info.3;

        let mean_rounded = format!("{:.1}", mean);

        print!("{}={}/{}/{},", key, station_info.0, mean_rounded, station_info.2);
        println!();
    }

    let duration = start.elapsed();
    
    println!("Time elapsed in the operation is: {:?}", duration);
    Ok(())
}