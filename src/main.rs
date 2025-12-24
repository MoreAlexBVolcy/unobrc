use std::fs::{self, File};
use std::time::Instant;
use std::collections::HashMap;
use std::collections::BTreeSet;
use std::io::{self, BufRead, BufReader};
use std::path::Path;

fn main() {
    read_file_with_buffer().expect("Failed to read file");
}

fn read_file_with_buffer() -> std::io::Result<()> {
    let start = Instant::now();

    // Specify file, and reader that'll speed up the reading process
    let f = File::open("./measurements.txt")?;
    let reader = BufReader::new(f);

    // Hash map to collect the data
    // StationName --> (min, mean, max, occur)
    let mut stations = HashMap::new();

    // List for sorting later
    let mut sorted_stations: BTreeSet<String> = BTreeSet::new();

    // Variables
    let mut l: String;
    let mut station_name: &str;
    let mut temp: f64;
    // let mut x = 0;

    // For each line, capture the station and the temp, store them in the map
    for line in reader.lines() {
        // if x == 100000 {
        //     break;
        // }
        // x += 1;

        l = line?;
        let mut parts = l.split(';');
        
        station_name = parts.next().unwrap();
        temp = parts.next().unwrap().parse::<f64>().unwrap();

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