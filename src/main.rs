use std::fs::{File};
use std::time::Instant;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::io::SeekFrom;
use std::io::Seek;
use std::thread;
use std::time::Duration;

fn main() {
    calculate_averages_threaded().expect("Failed to read file");
}


// Assign 4 threads there own file part
fn calculate_averages_threaded() -> std::io::Result<()> {
    let start = Instant::now();
    let mut buf = vec![];

    // File Reader 1
    let mut f1 = File::open("./measurements.txt")?;
    let mut f1_reader = BufReader::new(f1);
    let mut total_bytes_f1 = 0;

    // File Reader 2
    let mut f2 = File::open("./measurements.txt")?;
    f2.seek(SeekFrom::Start((3448862047)))?;
    let mut f2_reader = BufReader::new(f2);
    
    // Ensure we start at newline
    let mut total_bytes_f2 = f2_reader.read_until(10u8, &mut buf).unwrap();

    // File Reader 3
    let mut f3 = File::open("./measurements.txt")?;
    f3.seek(SeekFrom::Start((3448862047*2)))?;
    let mut f3_reader = BufReader::new(f3);
    let mut total_bytes_f3 = f3_reader.read_until(10u8, &mut buf).unwrap();

    // File Reader 4
    let mut f4 = File::open("./measurements.txt")?;
    f4.seek(SeekFrom::Start((3448862047*3)))?;
    let mut f4_reader = BufReader::new(f4);
    let _ = f4_reader.read_until(10u8, &mut buf).unwrap();


    // MAIN MAP: StationName --> (min, mean, max, occur)
    let mut stations_f1 = HashMap::new();
    let mut stations_f2 = HashMap::new();
    let mut stations_f3 = HashMap::new();
    let mut stations_f4 = HashMap::new();


    // T1
        let t1_handler = thread::spawn(move || {
            let mut line1 = String::new();
            let mut station_name: &str;
            let mut temp: f64;
            
            // Loop until we've read the right number of bytes
            loop {
                match f1_reader.read_line(&mut line1) {
                    Ok(bytes_read) => {
                        if bytes_read == 0 {
                            break;
                        }
                        let mut parts = line1.split(';');
                        station_name = parts.next().unwrap();
                        temp = parts.next().unwrap().trim().parse::<f64>().unwrap();

                        // F1 Map Manipulation
                        let station_info = stations_f1.entry(String::from(station_name)).or_insert((temp, temp, temp, 1.0));
                        
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
                        
                        line1.clear();

                        total_bytes_f1 += bytes_read;

                        if total_bytes_f1 >= 3448862047 {
                            break;
                        }
                    }
                    Err(err) => {
                        println!("Error occurred: {}", err);
                        break;
                    }
                }
            }
            stations_f1
    });

    // T2
        let t2_h = thread::spawn(move || {
            let mut line2 = String::new();
            let mut station_name: &str;
            let mut temp: f64;
            
            // Loop until we've read the right number of bytes
            loop {
                match f2_reader.read_line(&mut line2) {
                    Ok(bytes_read) => {
                        if bytes_read == 0 {
                            break;
                        }
                        let mut parts = line2.split(';');
                        station_name = parts.next().unwrap();
                        temp = parts.next().unwrap().trim().parse::<f64>().unwrap();

                        // F1 Map Manipulation
                        let station_info = stations_f2.entry(String::from(station_name)).or_insert((temp, temp, temp, 1.0));
                        
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
                        
                        line2.clear();

                        total_bytes_f2 += bytes_read;

                        if total_bytes_f2 >= 3448862047*2 {
                            break;
                        }

                    }
                    Err(err) => {
                        println!("Error occurred: {}", err);
                        break;
                    }
                }
            }
            stations_f2
    });

    // T3
        let t3_h = thread::spawn(move || {
            let mut line3 = String::new();
            let mut station_name: &str;
            let mut temp: f64;
            
            // Loop until we've read the right number of bytes
            loop {
                match f3_reader.read_line(&mut line3) {
                    Ok(bytes_read) => {
                        if bytes_read == 0 {
                            break;
                        }
                        let mut parts = line3.split(';');
                        station_name = parts.next().unwrap();
                        temp = parts.next().unwrap().trim().parse::<f64>().unwrap();

                        // F1 Map Manipulation
                        let station_info = stations_f3.entry(String::from(station_name)).or_insert((temp, temp, temp, 1.0));
                        
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
                        
                        line3.clear();

                        total_bytes_f3 += bytes_read;

                        if total_bytes_f3 >= 3448862047*3 {
                            break;
                        }

                    }
                    Err(err) => {
                        println!("Error occurred: {}", err);
                        break;
                    }
                }
            }
            stations_f3
    });

    // T4
        let t4_h = thread::spawn(move || {
            let mut line4 = String::new();
            let mut station_name: &str;
            let mut temp: f64;
            
            // Loop until we've read the right number of bytes
            loop {
                match f4_reader.read_line(&mut line4) {
                    Ok(bytes_read) => {
                        if bytes_read == 0 {
                            break;
                        }
                        let mut parts = line4.split(';');
                        station_name = parts.next().unwrap();
                        temp = parts.next().unwrap().trim().parse::<f64>().unwrap();

                        // F1 Map Manipulation
                        let station_info = stations_f4.entry(String::from(station_name)).or_insert((temp, temp, temp, 1.0));
                        
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
                        
                        line4.clear();

                    }
                    Err(err) => {
                        println!("Error occurred: {}", err);
                        break;
                    }
                }
            }
            stations_f4
    });

    let result1: HashMap<String, (f64, f64, f64, f64)> = t1_handler.join().unwrap();
    let mut result2 = t2_h.join().unwrap();
    let mut result3 = t3_h.join().unwrap();
    let mut result4 = t4_h.join().unwrap();

    // Map Merge
    for (key, value) in result1 {
    result2.entry(key)
        .and_modify(|existing| {
            // existing.0 = min, existing.1 = sum, existing.2 = max, existing.3 = count
            existing.0 = existing.0.min(value.0);
            existing.1 += value.1;
            existing.2 = existing.2.max(value.2);
            existing.3 += value.3;
        })
        .or_insert(value);  // If key doesn't exist, insert it
    }

    for (key, value) in result2 {
        result3.entry(key)
        .and_modify(|existing| {
            // existing.0 = min, existing.1 = sum, existing.2 = max, existing.3 = count
            existing.0 = existing.0.min(value.0);  // Update min
            existing.1 += value.1;                  // Add sums
            existing.2 = existing.2.max(value.2);  // Update max
            existing.3 += value.3;                  // Add counts
        })
        .or_insert(value);  // If key doesn't exist, insert it
    }


    for (key, value) in result3 {
        result4.entry(key)
        .and_modify(|existing| {
            // existing.0 = min, existing.1 = sum, existing.2 = max, existing.3 = count
            existing.0 = existing.0.min(value.0);  // Update min
            existing.1 += value.1;                  // Add sums
            existing.2 = existing.2.max(value.2);  // Update max
            existing.3 += value.3;                  // Add counts
        })
        .or_insert(value);  // If key doesn't exist, insert it
    }

    let mut sorted_stations: Vec<String> = Vec::new();
    for (key, _) in result4.iter() {
        sorted_stations.push(key.to_string());
    }
    sorted_stations.sort();

    for key in &sorted_stations {
        let station_info = result4[key];

        let mean = station_info.1 / station_info.3;

        let mean_rounded = format!("{:.1}", mean);

        print!("{}={}/{}/{},", key, station_info.0, mean_rounded, station_info.2);
        println!();
    }

    let duration = start.elapsed();
    
    println!("Time elapsed in the operation is: {:?}", duration);
    Ok(())
}