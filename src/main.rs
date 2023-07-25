
use std::{str::FromStr, collections::HashMap, hash::Hash};
use anyhow::{Result, anyhow};

use snp500_data;
use polars_core::prelude::*;


mod av;
mod data_manipulation;
// KEY: 8FCG2UU0IWQHWH6G


#[tokio::main]
async fn main() {
    let data = snp500_data::fetcher::snp_data().await.unwrap();
    let sector_groups = snp500_data::group::by_sector(&data);

    let groups = sector_groups.groups().unwrap();
    println!("{:?}", groups);
    // println!("{:?}", sector_groups.keys());
    // println!("{:?}", sector_groups.get_groups());

    let sector_data_map = data_manipulation::groupByToHashMap(data.clone(), sector_groups).await.unwrap();


    println!("HELLO: {:?}", sector_data_map.keys());
    for key in sector_data_map.keys().into_iter() {
        println!("{}, {:?}", key, sector_data_map.get(key).unwrap().len())
    }


    
    
}
