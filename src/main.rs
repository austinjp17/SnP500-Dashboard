
use snp500_data;
use polars_core::prelude::*;

mod av;

// KEY: 8FCG2UU0IWQHWH6G


#[tokio::main]
async fn main() {
    let data = snp500_data::fetcher::snp_data().await.unwrap();
    let sector_data = snp500_data::group::by_sector(&data);

    let key = "8FCG2UU0IWQHWH6G".to_string(); // TODO: abstract api key out

    let query = av::AvFunctionCall::TimeSeries { 
        step: av::TimeSeriesStep::Daily, 
        symbol: String::from("ibm"), 
        outputsize: None, 
        datatype: Some(String::from("csv")), 
        api_key: key,
    };

    let url = query.build_url()
        .expect("Error building url");

    let resp = snp500_data::request::basic(&url).await
        .expect(format!("Err on av api call, url called: {}", url).as_str());

    let data = av::time_series_parser(resp);
    println!("{:?}", data.unwrap().head(Some(5)));
    
}
