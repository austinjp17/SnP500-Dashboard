
use std::{str::FromStr, collections::HashMap, hash::Hash};
use anyhow::{Result, anyhow};

use snp500_data;
use polars_core::prelude::*;


mod av;
mod data_manipulation;
// KEY: 8FCG2UU0IWQHWH6G

fn combine_dfs(df1: &DataFrame, df2: &DataFrame) -> DataFrame {
    let combined_df = DataFrame::default();
            
    let deepest_df;
    if df1.height() > df2.height() {
        deepest_df = df1.height()
    } else {
        deepest_df = df2.height()
    }

    let select_cols = vec!["open", "high", "low", "close", "volumn"];
    
    let mut col_vec = vec![];
    for i in 0..select_cols.len() {
        

        let mut df1_col = df1
            .column(select_cols.get(i).unwrap()
        ).unwrap().clone();
        let mut df2_col = df2
            .column(select_cols.get(i).unwrap()
        ).unwrap().clone();
        
        println!("1- {}:{} | 2- {}:{}", 
            df1_col.name(),
            df1_col.dtype(),
            df2_col.name(), 
            df2_col.dtype()
        );
        if df1_col.len() != df2_col.len(){
            let len_diff = df1_col.len().abs_diff(df2_col.len());

            if df1_col.name() == "volumn" {
                let temp_vec = vec![0 as u32; len_diff];
                let addition = Series::new(select_cols.get(i).unwrap(), temp_vec);
                if df1.height() > df2.height() {
                    df2_col.append(&addition).unwrap();
                } 
                else {
                    df1_col.append(&addition).unwrap();
                }
            } else {
                let temp_vec = vec![0 as f64; len_diff];
                let addition = Series::new(select_cols.get(i).unwrap(), temp_vec);
                if df1.height() > df2.height() {
                    df2_col.append(&addition).unwrap();
                } 
                else {
                    df1_col.append(&addition).unwrap();
                }
            }
        // println!("Adj Col Len: {} | {}", df1_col.len(), df2_col.len());
        }
        let temp_series = df1_col + df2_col;
        col_vec.push(temp_series);   
    }
    // insert timestamp col
    if df1.height() == deepest_df {
        col_vec.insert(0, df1.column("timestamp").unwrap().clone())
    } else {
        col_vec.insert(0, df2.column("timestamp").unwrap().clone())
    }

    let res_df = DataFrame::new(
        col_vec
    ).unwrap();
    res_df

}


#[tokio::main]
async fn main() {
    let data = snp500_data::fetcher::snp_data().await.unwrap();
    let sector_groups = snp500_data::group::by_sector(&data);

    let groups = sector_groups.groups().unwrap();
    println!("{:?}", groups);
    // println!("{:?}", sector_groups.keys());
    // println!("{:?}", sector_groups.get_groups());

    let sector_data_map = data_manipulation::groupByToHashMap(data.clone(), sector_groups).await.unwrap();


    println!("Map Keys: {:?}", sector_data_map.keys());
    for key in sector_data_map.keys().into_iter() {
        println!("{} | {:?}", key, sector_data_map.get(key).unwrap().len())
    }
    
    for key in sector_data_map.keys() {
        let mut queue = sector_data_map.get(key).unwrap().clone();
        let init_len = queue.len();
        while queue.len() > 1 {
            // queue.pop();
            let df1 = queue.pop().unwrap();
            let df2 = queue.pop().unwrap();
            println!("DF Heights: {} | {}", df1.height(), df2.height());
            let res_df = combine_dfs(&df1, &df2);

            println!("{},{:?}, {:?}", df1, df2, res_df);
        }     
            
        
    }

}
