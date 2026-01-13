use crate::Result as CrateResult;
use serde::Deserialize;
use std::str::FromStr;

pub type Amount = i64;
const SCALE: i64 = 10_000;

fn parse_amount(raw: &str) -> CrateResult<Amount> {
    let decimal = raw.trim().parse::<f64>()?;
    Ok((decimal * SCALE as f64).round() as i64)
}

fn amount_from_str<'de, D>(deserializer: D) -> Result<Option<Amount>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    opt.map(|s| parse_amount(&s).map_err(serde::de::Error::custom))
        .transpose()
}

#[derive(Deserialize, Debug)]
pub struct Transaction {
    #[serde(rename = "type")]
    pub kind: Kind,
    pub client: u16,
    pub tx: u32,
    #[serde(deserialize_with = "amount_from_str")]
    pub amount: Option<Amount>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    #[serde(rename = "chargeback")]
    ChargeBack,
}

impl FromStr for Kind {
    type Err = ();

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim().to_lowercase().as_str() {
            "deposit" => Ok(Self::Deposit),
            "withdrawal" => Ok(Self::Withdrawal),
            "dispute" => Ok(Self::Dispute),
            "resolve" => Ok(Self::Resolve),
            "chargeback" => Ok(Self::ChargeBack),
            _ => Err(()),
        }
    }
}
