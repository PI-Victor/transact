use crate::Result as CrateResult;
use serde::Deserialize;
use std::str::FromStr;

pub type Amount = i64;
pub const SCALE: i64 = 10_000;

fn parse_amount(raw: &str) -> CrateResult<Amount> {
    let decimal = raw.trim().parse::<f64>()?;
    Ok((decimal * SCALE as f64).round() as i64)
}

pub fn format_amount(value: Amount) -> String {
    let sign = if value < 0 { "-" } else { "" };
    let abs = value.abs();
    let whole = abs / SCALE;
    let frac = abs % SCALE;
    format!("{sign}{whole}.{frac:04}")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_amount_handles_whitespace_and_precision() {
        assert_eq!(super::parse_amount("1.2345").unwrap(), 12_345);
        assert_eq!(super::parse_amount("  0.0001 ").unwrap(), 1);
        assert_eq!(super::parse_amount("2").unwrap(), 20_000);
    }

    #[test]
    fn format_amount_round_trips_values() {
        let samples = [0, 1, 12_345, -12_345, 200_000];
        for &value in &samples {
            let formatted = format_amount(value);
            let reparsed = super::parse_amount(&formatted).unwrap();
            assert_eq!(value, reparsed);
        }
    }

    #[test]
    fn transaction_deserializes_from_csv_row() {
        let csv = "type,client,tx,amount\nwithdrawal,42,7,1.5000\n";
        let mut rdr = csv::Reader::from_reader(csv.as_bytes());
        let mut iter = rdr.deserialize::<Transaction>();
        let tx = iter.next().unwrap().unwrap();
        matches!(tx.kind, Kind::Withdrawal);
        assert_eq!(tx.client, 42);
        assert_eq!(tx.tx, 7);
        assert_eq!(tx.amount, Some(15_000));
    }
}
