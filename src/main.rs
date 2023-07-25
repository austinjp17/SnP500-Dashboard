
use std::{str::FromStr, collections::HashMap, hash::{Hash, self}};
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

    println!("{:?}", sector_groups.groups().unwrap());
    // println!("{:?}", sector_groups.keys());
    // println!("{:?}", sector_groups.get_groups());

    let mut sector_data_map: HashMap<String, Vec<DataFrame>> = data_manipulation::groupByToHashMap(data.clone(), sector_groups).await.unwrap();


    println!("Map Keys: {:?}", sector_data_map.keys());
    for key in sector_data_map.keys().into_iter() {
        println!("{} | {:?} Elements", key, sector_data_map.get(key).unwrap().len())
    }

    let perc_hashmap = data_manipulation::to_pctchg_hashmap(&mut sector_data_map);
    println!("{}", perc_hashmap.get("Energy").unwrap().len());
    
    

    // data_manipulation::avg_dfs(sector_data_map);
    
    

}
