use std::time::Duration;

use anyhow::{ensure, Context, Result};
use deadpool_postgres::Pool;
use serde::Deserialize;
use serde_json::{json, Value};

const OFF_LICENSE_ID: &str = "ODbL-1.0+DbCL-1.0";
const OFF_LICENSE_URI: &str = "https://opendatacommons.org/licenses/odbl/1-0/";
const USDA_LICENSE_ID: &str = "CC0-1.0";
const USDA_LICENSE_URI: &str = "https://creativecommons.org/publicdomain/zero/1.0/";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Search {
    query: String,
}

pub async fn lookup(pool: &Pool, input: Search) -> Result<Value> {
    let query = input.query.trim();
    ensure!(
        (2..=100).contains(&query.chars().count()),
        "invalid food search"
    );
    let now = chrono::Utc::now();
    let fresh_after = now - chrono::Duration::hours(24);
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(7))
        .redirect(reqwest::redirect::Policy::none())
        .user_agent("OpenHuman-Avatar/0.1 (https://github.com/tinyhumansai/openhuman)")
        .build()?;
    if matches!(query.len(), 8 | 12 | 13 | 14) && query.bytes().all(|byte| byte.is_ascii_digit()) {
        let cached =
            crate::catalog_store::find(pool, "open_food_facts", query, Some(fresh_after)).await?;
        if !cached.is_empty() {
            return Ok(json!({"status":"ready","foods":cached}));
        }
        let response = match http
            .get(format!(
                "https://world.openfoodfacts.org/api/v3/product/{query}"
            ))
            .query(&[("fields", "code,product_name,product_name_ko,nutriments")])
            .send()
            .await
        {
            Ok(response) => response,
            Err(_) => {
                let stale =
                    crate::catalog_store::find(pool, "open_food_facts", query, None).await?;
                return Ok(
                    json!({"status": if stale.is_empty() {"unavailable"} else {"stale_cache"}, "foods":stale}),
                );
            }
        };
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            let stale = crate::catalog_store::find(pool, "open_food_facts", query, None).await?;
            return Ok(
                json!({"status": if stale.is_empty() {"no_records"} else {"stale_cache"}, "foods":stale}),
            );
        }
        let body: Value = match response.error_for_status() {
            Ok(response) => match response.json().await {
                Ok(body) => body,
                Err(_) => {
                    let stale =
                        crate::catalog_store::find(pool, "open_food_facts", query, None).await?;
                    return Ok(
                        json!({"status": if stale.is_empty() {"unavailable"} else {"stale_cache"}, "foods":stale}),
                    );
                }
            },
            Err(_) => {
                let stale =
                    crate::catalog_store::find(pool, "open_food_facts", query, None).await?;
                return Ok(
                    json!({"status": if stale.is_empty() {"unavailable"} else {"stale_cache"}, "foods":stale}),
                );
            }
        };
        let product = &body["product"];
        let name = product["product_name_ko"]
            .as_str()
            .filter(|value| !value.is_empty())
            .or_else(|| {
                product["product_name"]
                    .as_str()
                    .filter(|value| !value.is_empty())
            });
        let foods = name
            .into_iter()
            .map(|name| map_open_food_facts_product(product, query, name, chrono::Utc::now()))
            .collect::<Vec<_>>();
        crate::catalog_store::save(pool, &foods).await?;
        return Ok(result(foods));
    }

    if let Some(key) = std::env::var("AVATAR_MFDS_API_KEY")
        .ok()
        .filter(|key| !key.trim().is_empty())
    {
        let cached = crate::catalog_store::find(pool, "mfds", query, Some(fresh_after)).await?;
        if !cached.is_empty() {
            return Ok(json!({"status":"ready","foods":cached}));
        }
        let response = http
            .get("https://apis.data.go.kr/1471000/FoodNtrCpntDbInfo03/getFoodNtrCpntDbInq03")
            .query(&[
                ("serviceKey", key.as_str()),
                ("type", "json"),
                ("pageNo", "1"),
                ("numOfRows", "10"),
                ("FOOD_NM_KR", query),
            ])
            .send()
            .await;
        if let Ok(response) = response {
            if let Ok(body) = response.error_for_status() {
                if let Ok(body) = body.json::<Value>().await {
                    let rows = &body["body"]["items"];
                    let rows = rows.as_array().cloned().unwrap_or_else(|| {
                        rows.as_object()
                            .map(|item| vec![Value::Object(item.clone())])
                            .unwrap_or_default()
                    });
                    let foods: Vec<_> = rows
                        .iter()
                        .filter_map(|row| map_mfds_food(row, chrono::Utc::now()))
                        .collect();
                    crate::catalog_store::save(pool, &foods).await?;
                    if !foods.is_empty() {
                        return Ok(result(foods));
                    }
                }
            }
        }
    }

    let cached = crate::catalog_store::find(pool, "usda", query, Some(fresh_after)).await?;
    if !cached.is_empty() {
        return Ok(json!({"status":"ready","foods":cached}));
    }

    let Some(key) = std::env::var("AVATAR_USDA_API_KEY")
        .ok()
        .filter(|key| !key.trim().is_empty())
    else {
        let stale = crate::catalog_store::find(pool, "usda", query, None).await?;
        return Ok(
            json!({"status": if stale.is_empty() {"configuration_required"} else {"stale_cache"}, "foods":stale}),
        );
    };
    // Use a header to keep the credential out of the request URL.
    let response = match http
        .get("https://api.nal.usda.gov/fdc/v1/foods/search")
        .header("X-Api-Key", key)
        .query(&[("query", query), ("pageSize", "10")])
        .send()
        .await
    {
        Ok(response) => response,
        Err(_) => {
            let stale = crate::catalog_store::find(pool, "usda", query, None).await?;
            return Ok(
                json!({"status": if stale.is_empty() {"unavailable"} else {"stale_cache"}, "foods":stale}),
            );
        }
    };
    let body: Value = match response.error_for_status() {
        Ok(response) => match response.json().await {
            Ok(body) => body,
            Err(_) => {
                let stale = crate::catalog_store::find(pool, "usda", query, None).await?;
                return Ok(
                    json!({"status": if stale.is_empty() {"unavailable"} else {"stale_cache"}, "foods":stale}),
                );
            }
        },
        Err(_) => {
            let stale = crate::catalog_store::find(pool, "usda", query, None).await?;
            return Ok(
                json!({"status": if stale.is_empty() {"unavailable"} else {"stale_cache"}, "foods":stale}),
            );
        }
    };
    let foods: Vec<Value> = body["foods"]
        .as_array()
        .context("invalid FoodData Central response")?
        .iter()
        .take(10)
        .filter_map(|food| map_usda_food(food, chrono::Utc::now()))
        .collect();
    crate::catalog_store::save(pool, &foods).await?;
    Ok(result(foods))
}

fn result(foods: Vec<Value>) -> Value {
    json!({"status": if foods.is_empty() {"no_records"} else {"ready"}, "foods": foods})
}

fn map_usda_food(food: &Value, retrieved_at: chrono::DateTime<chrono::Utc>) -> Option<Value> {
    let id = food["fdcId"].as_u64()?.to_string();
    let name = food["description"].as_str()?.trim();
    if name.is_empty() {
        return None;
    }
    let food_nutrients = food["foodNutrients"]
        .as_array()
        .map(|nutrients| Value::Array(nutrients.iter().take(100).cloned().collect()))
        .unwrap_or_else(|| Value::Array(Vec::new()));
    let nutrient = |id: u64| {
        food["foodNutrients"]
            .as_array()?
            .iter()
            .find(|item| item["nutrientId"].as_u64() == Some(id))
            .and_then(|item| item["value"].as_f64())
    };
    Some(json!({
        "source":"usda", "source_id":id, "name":name,
        "basis_amount":100.0,"basis_unit":"g",
        "nutrients_per_basis":{"kcal":nutrient(1008),"protein_g":nutrient(1003),"carbs_g":nutrient(1005),"fat_g":nutrient(1004)},
        "food_nutrients": food_nutrients,
        "source_uri":format!("https://fdc.nal.usda.gov/food-details/{id}/nutrients"),
        "license_id":USDA_LICENSE_ID, "license_uri":USDA_LICENSE_URI,
        "retrieved_at":retrieved_at.to_rfc3339(),
    }))
}

fn map_mfds_food(food: &Value, retrieved_at: chrono::DateTime<chrono::Utc>) -> Option<Value> {
    let id = food["FOOD_CD"]
        .as_str()
        .or_else(|| food["foodCd"].as_str())?;
    let name = food["FOOD_NM_KR"]
        .as_str()
        .or_else(|| food["foodNm"].as_str())?;
    let number = |keys: &[&str]| {
        keys.iter().find_map(|key| {
            food[*key]
                .as_str()
                .and_then(|value| value.parse::<f64>().ok())
                .or_else(|| food[*key].as_f64())
        })
    };
    let (basis, basis_unit) = parse_mfds_basis(&food["SERVING_SIZE"])?;
    if basis <= 0.0 {
        return None;
    }
    let nutrients = json!({"kcal":number(&["AMT_NUM1","ENERGY_KCAL"]),"protein_g":number(&["AMT_NUM3","PROTEIN"]),"fat_g":number(&["AMT_NUM4","FAT"]),"carbs_g":number(&["AMT_NUM6","CARBOHYDRATE"])});
    let kcal_per_100g = (basis_unit == "g")
        .then(|| nutrients["kcal"].as_f64().map(|kcal| kcal * 100.0 / basis))
        .flatten();
    Some(
        json!({"source":"mfds","source_id":id,"name":name,"basis_amount":basis,"basis_unit":basis_unit,"kcal_per_100g":kcal_per_100g,
        "nutrients_per_basis":nutrients,
        "source_uri":"https://www.data.go.kr/data/15127578/openapi.do","license_id":"data.go.kr-no-restriction","license_uri":"https://www.data.go.kr/ugs/selectPortalPolicyView.do","retrieved_at":retrieved_at.to_rfc3339()}),
    )
}

fn parse_mfds_basis(value: &Value) -> Option<(f64, &'static str)> {
    if let Some(amount) = value.as_f64() {
        return Some((amount, "g"));
    }
    let raw = value.as_str()?.trim().to_ascii_lowercase().replace(' ', "");
    let unit = if raw.ends_with("ml") {
        "ml"
    } else if raw.ends_with('g') {
        "g"
    } else {
        return None;
    };
    let amount = raw.strip_suffix(unit)?.parse::<f64>().ok()?;
    amount.is_finite().then_some((amount, unit))
}

fn map_open_food_facts_product(
    product: &Value,
    barcode: &str,
    name: &str,
    retrieved_at: chrono::DateTime<chrono::Utc>,
) -> Value {
    json!({
        "source":"open_food_facts", "source_id":barcode, "name":name,
        // Preserve source values and unit-bearing keys without guessing conversions.
        "nutriments":product["nutriments"],
        "basis_amount":100.0,"basis_unit":"g",
        "nutrients_per_basis":{"kcal":product["nutriments"]["energy-kcal_100g"],"protein_g":product["nutriments"]["proteins_100g"],"carbs_g":product["nutriments"]["carbohydrates_100g"],"fat_g":product["nutriments"]["fat_100g"]},
        "source_uri":format!("https://world.openfoodfacts.org/product/{barcode}"),
        "license_id":OFF_LICENSE_ID, "license_uri":OFF_LICENSE_URI,
        "retrieved_at":retrieved_at.to_rfc3339(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_food_facts_keeps_source_nutrients_and_unit_keys() {
        let retrieved_at = chrono::DateTime::parse_from_rfc3339("2026-11-26T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let product = json!({
            "nutriments": {
                "energy-kcal_100g": 240,
                "proteins_100g": 8.5,
                "proteins_unit": "g",
                "carbohydrates_100g": 31.0,
                "carbohydrates_unit": "g",
                "sodium_100g": 0.42,
                "sodium_unit": "g"
            }
        });
        let mapped =
            map_open_food_facts_product(&product, "0123456789012", "Example", retrieved_at);
        assert_eq!(mapped["nutriments"]["proteins_100g"], 8.5);
        assert_eq!(mapped["nutriments"]["proteins_unit"], "g");
        assert_eq!(mapped["nutriments"]["carbohydrates_100g"], 31.0);
        assert_eq!(mapped["nutriments"]["sodium_100g"], 0.42);
        assert_eq!(mapped["license_id"], "ODbL-1.0+DbCL-1.0");
    }

    #[test]
    fn usda_results_keep_source_nutrient_values_and_units() {
        let retrieved_at = chrono::DateTime::parse_from_rfc3339("2026-11-26T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let row = json!({
            "fdcId": 12345,
            "description": "Example food",
            "foodNutrients": [{
                "nutrientId": 1003,
                "nutrientName": "Protein",
                "value": 12.5,
                "unitName": "G"
            }]
        });
        let mapped = map_usda_food(&row, retrieved_at).unwrap();
        assert_eq!(mapped["food_nutrients"][0]["nutrientId"], 1003);
        assert_eq!(mapped["food_nutrients"][0]["value"], 12.5);
        assert_eq!(mapped["food_nutrients"][0]["unitName"], "G");
        assert_eq!(mapped["license_id"], "CC0-1.0");
        assert_eq!(mapped["source_id"], "12345");
    }

    #[test]
    fn mfds_food_uses_the_reported_basis_and_food_code() {
        let retrieved_at = chrono::DateTime::parse_from_rfc3339("2026-11-26T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let row = json!({
            "FOOD_CD":"RICE-TEST", "FOOD_NM_KR":"현미밥", "SERVING_SIZE":"100g",
            "AMT_NUM1":"130", "AMT_NUM3":"2.6", "AMT_NUM4":"1.0", "AMT_NUM6":"28.6"
        });
        let mapped = map_mfds_food(&row, retrieved_at).unwrap();
        assert_eq!(mapped["source"], "mfds");
        assert_eq!(mapped["source_id"], "RICE-TEST");
        assert_eq!(mapped["basis_amount"], 100.0);
        assert_eq!(mapped["basis_unit"], "g");
        assert_eq!(mapped["kcal_per_100g"], 130.0);
        assert_eq!(mapped["nutrients_per_basis"]["protein_g"], 2.6);
        assert_eq!(mapped["nutrients_per_basis"]["carbs_g"], 28.6);
        assert_eq!(mapped["license_id"], "data.go.kr-no-restriction");
        let beverage = map_mfds_food(&json!({
            "FOOD_CD":"DRINK-TEST", "FOOD_NM_KR":"음료", "SERVING_SIZE":"100 mL", "AMT_NUM1":"42"
        }), retrieved_at).unwrap();
        assert_eq!(beverage["basis_amount"], 100.0);
        assert_eq!(beverage["basis_unit"], "ml");
        assert!(beverage["kcal_per_100g"].is_null());
    }
}
