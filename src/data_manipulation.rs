use polars::prelude::GroupBy;
use polars::datatypes::AnyValue::Utf8;
use polars::datatypes::AnyValue::UInt32;
use anyhow::{Result, anyhow};
use polars_core::prelude::*;
use std::{collections::HashMap};
use crate::av;

pub async fn groupByToHashMap(
    data:DataFrame, 
    groups: GroupBy<'_>
) -> Result<HashMap<String, Vec<DataFrame>>> {
    // initialize data vectors
    let mut sector_data_map:HashMap<String, Vec<DataFrame>> = HashMap::new();
    let keys = &groups.keys()[0];
    let vals = groups.get_groups().as_list_chunked();

    for i in 0..keys.len() 
    {
        let hash_key = keys.get(i).unwrap();
        let group_indexes = vals.get(i).unwrap();

        // sector lock
        if hash_key.get_str().unwrap() != "Energy" {
            continue;
        } 
        match hash_key {
            Utf8(sector) => {
                println!("{}", sector);
                for company_index in group_indexes.iter() {
                    
                    match company_index {
                        UInt32(index) => {
                            
                            let company_symbol = data.column("symbol").unwrap().get(index as usize).unwrap();
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
                        _ => {return Err(anyhow!("Unexpected Company Index type"))}
                    }
                    // println!("{:?}", company_index);
                }
                

                // sector_data_map.insert(sector.to_string(), )
            }
            _ => {println!("Unexpected type in hash key, {}", hash_key)}
        }
        
    }
    Ok(sector_data_map)
}