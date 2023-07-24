
use std::str::FromStr;

use snp500_data;
use polars_core::prelude::*;

mod av;

// KEY: 8FCG2UU0IWQHWH6G


#[tokio::main]
async fn main() {
    let data = snp500_data::fetcher::snp_data().await.unwrap();
    let sector_data = snp500_data::group::by_sector(&data);

    let key = "8FCG2UU0IWQHWH6G".to_string(); // TODO: abstract api key out

    use polars::datatypes::AnyValue::Utf8;
    let symbol_col = data.column("symbol").unwrap();
    symbol_col.utf8().expect("Not strings?");
    for elem in symbol_col.iter() {
        match elem {
            Utf8(symbol) => {

                println!("--- {} ---", symbol);

                let query = av::AvFunctionCall::TimeSeries { 
                    step: av::TimeSeriesStep::Daily, 
                    symbol: String::from_str(symbol).unwrap(), 
                    outputsize: None, 
                    datatype: av::AvDatatype::Csv, 
                    api_key: "8FCG2UU0IWQHWH6G".to_string(),
                };

                let data = query.send_request().await.unwrap();
                println!("{}", data.head(Some(5)));
            }
            _ => {println!("Err")}
        }
    };
    
    // for i in 0..20 {
        
        
        // let query = av::AvFunctionCall::TimeSeries { 
        //     step: av::TimeSeriesStep::Daily, 
        //     symbol: symbol.to_string(), 
        //     outputsize: None, 
        //     datatype: av::AvDatatype::Csv, 
        //     api_key: "8FCG2UU0IWQHWH6G".to_string(),
        // };

        // query.print_built_url();
        // let url = query.build_url()
        //     .expect("Error building url");

        // let resp = snp500_data::request::basic(&url).await
        //     .expect(format!("Err on av api call, url called: {}", url).as_str());

        // let data = av::time_series_parser(resp);
        
    // }
    // println!("{:?}", data.head(Some(5)));
    
}
