
use std::{str::FromStr, collections::HashMap, hash::Hash};
use anyhow::{Result, anyhow};
use snp500_data;
use polars_core::prelude::*;
use polars::datatypes::AnyValue::UInt32;
use polars::datatypes::AnyValue::Utf8;
use polars_core::frame::groupby::GroupBy;
mod av;

// KEY: 8FCG2UU0IWQHWH6G

// TODO: add sector lock to param
pub async fn groupBy_to_hashmap(groups: GroupBy<'_>) -> HashMap<String, Vec<DataFrame>> {
    let snp_data = snp500_data::fetcher::snp_data().await.unwrap();
    let mut sector_data_map:HashMap<String, Vec<DataFrame>> = HashMap::new();
    let keys = &groups.keys()[0];
    let vals = groups.get_groups().as_list_chunked();

    for i in 0..keys.len() {
        let mut hash_key = keys.get(i).unwrap();
        let group_indexes = vals.get(i).unwrap();

        // sector lock
        if hash_key.get_str().unwrap() != "Financials" {
            continue;
        }

        match hash_key {
            Utf8(sector) => {
                println!("{}", sector);
                for company_index in group_indexes.iter() {
                    
                    match company_index {
                        UInt32(index) => {
                            
                            let company_symbol = snp_data.column("symbol").unwrap().get(index as usize).unwrap();
                            // println!("{}", company_symbol);
                            let company_data = av::get_comp_data(company_symbol).await.unwrap();
                            // println!("{}", company_data.head(Some(5)));

                            if sector_data_map.contains_key(sector) {
                                let mut sector_vals = sector_data_map.get_mut(sector).unwrap();
                                sector_vals.push(company_data);
                            } else {
                                sector_data_map.insert(sector.to_string(), vec![company_data]);
                            }
                        }
                        _ => {println!("{}", company_index.dtype())}
                    }
                    // println!("{:?}", company_index);
                }
                

                // sector_data_map.insert(sector.to_string(), )
            }
            _ => {println!("Unexpected type in hash key, {}", hash_key)}
        }
        
    }
    sector_data_map
}


#[tokio::main]
async fn main() {
    let data = snp500_data::fetcher::snp_data().await.unwrap();
    let sector_groups = snp500_data::group::by_sector(&data);

    let groups = sector_groups.groups().unwrap();
    println!("{:?}", groups);
    // println!("{:?}", sector_groups.keys());
    // println!("{:?}", sector_groups.get_groups());

    let sector_data_map = groupBy_to_hashmap(sector_groups).await;
    
    println!("HELLO: {:?}", sector_data_map.keys());
    for key in sector_data_map.keys().into_iter() {
        println!("{}, {:?}", key, sector_data_map.get(key).unwrap().len())
    }
    // println!("{}", keys.len())

    // let symbol_col = data.column("symbol").unwrap();
    // symbol_col.utf8().expect("Not strings?");

    // for elem in symbol_col.iter() {
    //     get_comp_data(elem).await;
    // }

    
    
}
