use polars_core::prelude::*;
use anyhow::{Result, anyhow};
use chrono::NaiveDate;
use polars::datatypes::AnyValue::Utf8;
use polars::datatypes::AnyValue::UInt32;
use std::{str::FromStr};

pub enum TimeSeriesStep {
    Daily,
    Weekly,
    Monthly
}
pub enum AvDatatype {
    Json,
    Csv,
    None
}
pub enum AvOutputSize {
    Compact,
    Full,
    None
}

pub enum AvFunctionCall {
    TimeSeries{
        step: TimeSeriesStep,
        symbol: String,
        outputsize: AvOutputSize, // compact || full
        datatype: AvDatatype,
        api_key: String,
    }, 
}
impl AvFunctionCall {
    pub fn build_url(&self) -> Result<String> {
        let mut query_url = String::from(
            "https://www.alphavantage.co/query?"
        );
        
        match self {
            AvFunctionCall::TimeSeries { 
                step, 
                symbol, 
                outputsize, 
                datatype, 
                api_key 
            } => {
    
                // append function call
                match step {
                    TimeSeriesStep::Daily => {
                        query_url.push_str("function=TIME_SERIES_DAILY");
                    },
                    TimeSeriesStep::Weekly => {
                        query_url.push_str("function=TIME_SERIES_WEEKLY_ADJUSTED");
                    },
                    TimeSeriesStep::Monthly => {
                        query_url.push_str("function=TIME_SERIES_MONTHLY_ADJUSTED");
                    }
                }
    
                // append query symbol
                // rm class a/b tags

                let mut symbol_param = symbol.to_lowercase();
                if symbol_param.contains('(') {
                    let start_i = symbol_param.find('(').unwrap();
                    // println!("PAR START: {}", start_i);
                    symbol_param = symbol_param[..start_i].trim().to_string();
                }

                if symbol_param.contains('.') {
                    symbol_param = symbol_param.replace('.', "-");
                }
                let symbol_query = format!("&symbol={}", symbol_param);
                query_url.push_str(&symbol_query);
    
                // append outputsize
                match step {
                    TimeSeriesStep::Daily => {
                        match outputsize {
                            AvOutputSize::Compact => {
                                let size_query = format!("&outputsize=compact");
                                query_url.push_str(&size_query);
                            }
                            AvOutputSize::Full => {
                                let size_query = format!("&outputsize=full");
                                query_url.push_str(&size_query);
                            }
                            _ => {}
                        }
                    }
                    TimeSeriesStep::Weekly => {
                        // match outputsize {
                        //     Some(_) => {
                        //         println!("Output size param not used for Weekly query.");
                        //     }
                        //     None => {}
                        // }
                    }
                    TimeSeriesStep::Monthly => {
                    //     match outputsize {
                    //         Some(_) => {
                    //             println!("Output size param not used for Monthly query.");
                    //         }
                    //         None => {}
                    //     }
                    }
                }
                
                // append datatype
                match datatype {
                    AvDatatype::Csv => {
                        let size_query = "&datatype=csv";
                        query_url.push_str(size_query);
                    }
                    AvDatatype::Json => {
                        let size_query = "&datatype=json";
                        query_url.push_str(size_query);
                    }
                    AvDatatype::None => {}
                }
    
                // append apikey
                let key_query = format!("&apikey={api_key}");
                query_url.push_str(&key_query);
            }
        }
        Ok(query_url)
    }

    pub fn print_built_url(&self) {
        println!("{:?}", self.build_url())
    }

    /// Builds URL and requests data from AlphaVantage
    pub async fn send_request(&self) -> Result<DataFrame> {
        let formatted_url = self.build_url();
        match formatted_url {
            Ok(url) => {
                // println!("{}", url);
                let mut recieved: bool = false;
                let mut resp_data = String::new();
                for i in 0..2 {
                    let resp = snp500_data::request::basic(&url).await;
                    match resp {
                        Ok(data) => {
                            recieved = true;
                            resp_data = data;
                        }
                        Err(e) => {
                            if i == 2 {
                                return Err(anyhow!("API Call Error: {}", e));
                            }
                        }
                    }
                    if recieved{
                        break;
                    }
                }
                
                        // println!("AV RESP: {}", resp);
                let formatted = csv_time_series_parser(resp_data);

                match formatted {
                    Ok(ret_data) => {
                        return Ok(ret_data);
                    }
                    Err(e) => {
                        return Err(anyhow!("Error building url: {}", e));
                    } 
                }
            }
            Err(e) => {
                return Err(anyhow!("Error building url: {}", e));
            }
        }
    }
}

pub async fn get_comp_data(symb_obj: AnyValue<'_>) -> Result<DataFrame>  {
    match symb_obj {
        Utf8(symbol) => {

            // println!("--- {} ---", symbol);

            let query = AvFunctionCall::TimeSeries { 
                step: TimeSeriesStep::Daily, 
                symbol: String::from_str(symbol).unwrap(), 
                outputsize: AvOutputSize::Full, 
                datatype: AvDatatype::Csv, 
                api_key: "8FCG2UU0IWQHWH6G".to_string(),
            };

            let data = query.send_request().await.unwrap();
            // println!("{}", data.head(Some(5)));
            return Ok(data);
        }
        _ => {
            return Err(anyhow!("Error getting {}", symb_obj));
    }};
    
}

pub fn csv_time_series_parser(csv_str: String) -> Result<DataFrame> {
    // df col initialization
    let mut timestamps_str = vec![];
    let mut opens:Vec<f64> = vec![];
    let mut highs:Vec<f64> = vec![];
    let mut lows:Vec<f64> = vec![];
    let mut closes:Vec<f64> = vec![];
    let mut volumns:Vec<f64> = vec![];

    let mut rdr = csv::Reader::from_reader(csv_str.as_bytes());
    for row in rdr.records() {
        match row {
            Ok(data) => {
                // println!("{:?}", data);
                if data.len() > 2{
                    timestamps_str.push(data.get(0).unwrap().to_string());
                    opens.push(data.get(1).unwrap().parse().unwrap());
                    highs.push(data.get(2).unwrap().parse().unwrap());
                    lows.push(data.get(3).unwrap().parse().unwrap());
                    closes.push(data.get(4).unwrap().parse().unwrap());
                    volumns.push(data.get(5).unwrap().parse().unwrap());
                } else {
                    return Err(anyhow!("Error parsing line: {:?}", data));
                }
                
            }
            Err(e) => {
                println!("Err parsing csv to vecs");
            }
        }
    }

    // timestap to date objs
    let mut timestamps = vec![];
    let fmt = "%Y-%m-%d";
    for i in 0..timestamps_str.len() {
        if let Ok(date_obj) = NaiveDate::parse_from_str(timestamps_str.get(i).unwrap(), fmt) {
            timestamps.push(Some(date_obj))
        } else {
            timestamps.push(None)
        }
    }

    let timestamps = Series::new("timestamp", timestamps);
    let opens = Series::new("open", opens);
    let highs = Series::new("high", highs);
    let lows = Series::new("low", lows);
    let closes = Series::new("close", closes);
    let volumns = Series::new("volumn", volumns);

    let data = DataFrame::new(
        vec![timestamps, opens, highs, lows, closes, volumns],
    );

    // println!("{:?}", data.unwrap().sample_n(10, false, true, None));
    

    Ok(data.unwrap().reverse())
}

