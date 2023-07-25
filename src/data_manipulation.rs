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


pub fn avg_dfs(sector_data_map: HashMap<String, Vec<DataFrame>>) {
    for key in sector_data_map.keys() {
        let mut queue = sector_data_map.get(key).unwrap().clone();
        let init_len = queue.len();
        println!("Init Queue: {}", queue.len());
        while queue.len() > 1 {
            // queue.pop();
            let df1 = queue.pop().unwrap();
            let df2 = queue.pop().unwrap();
            // println!("DF Heights: {} | {}", df1.height(), df2.height());
            let res_df = avg_dfs_helper(&df1, &df2);
            // println!("{},{:?}, {:?}", df1, df2, res_df);
            queue.push(res_df);
            // println!("Queue Len: {}", queue.len());
            
        }

        println!("{}", queue.get(0).unwrap().head(Some(5)));

        // divide col by count
        let mut res = queue.get(0).unwrap().clone();
        res.apply("open", |nums| {
            nums.f64().unwrap()
                .into_iter()
                .map(|opt_num| {
                    opt_num.map(|num| num/(init_len as f64))
                })
                .collect::<Float64Chunked>()
                .into_series()
        }).unwrap();

        println!("{}\nInit Len: {}", res.head(Some(5)), init_len);
            
        
    }
}

fn avg_dfs_helper(df1: &DataFrame, df2: &DataFrame) -> DataFrame {
            
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
        
        // println!("1- {}:{} | 2- {}:{}", 
        //     df1_col.name(),
        //     df1_col.dtype(),
        //     df2_col.name(), 
        //     df2_col.dtype()
        // );
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

pub fn to_pctchg_hashmap(hashmap: &mut HashMap<String, Vec<DataFrame>>) -> &mut HashMap<String, Vec<DataFrame>> {
    let select_cols = vec!["open", "high", "low", "close", "volumn"];
    let mut df = hashmap.get("Energy").unwrap().get(0).unwrap().clone();
    println!("{}", df.head(Some(5)));

    hashmap.clone().into_keys().for_each(|key| {
        let mut df_arr: &mut Vec<DataFrame> = hashmap.get_mut(&key).unwrap();
        df_arr.iter_mut().for_each(|df| {
            // println!("{}", df.head(Some(5)));
            select_cols.clone().into_iter().for_each(|row_name| {
                df.apply(row_name, data_manipulation::to_percent_chg_f64);
            });
            // println!("{}", df.head(Some(5)));
            // println!("----");
        })
    });

    hashmap
}

// APPLY FUNCTIONS

pub fn to_percent_chg_f64(num_vals: &Series) -> Series {
    let values = num_vals.f64().unwrap();
    let prev_values = values.shift(1);
    let mut first_element = true;

    let percentage_change = values
        .into_iter()
        .zip(prev_values.into_iter())
        .map(|(opt_val, opt_prev_val)| {
            match (opt_val, opt_prev_val) {
                (Some(val), Some(prev_val)) => {
                    let percentage = (val - prev_val) / prev_val * 100.0;
                    Some(percentage)
                }
                (Some(val), None) if first_element => {
                    first_element = false;
                    Some(0.0)
                }
                _ => None,
            }
        })
        .collect::<Float64Chunked>();

    percentage_change.into_series()
}





