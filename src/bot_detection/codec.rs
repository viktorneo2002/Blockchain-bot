// Serde-safe codecs: UnixMillis and Pubkey base58 wrappers
use serde::{Deserialize, Deserializer, Serializer};
use std::time::{SystemTime, UNIX_EPOCH};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct UnixMillis(pub u64);

impl UnixMillis {
    #[inline]
    pub fn now() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        UnixMillis(now.as_millis() as u64)
    }
}

impl serde::Serialize for UnixMillis {
    fn serialize<S: Serializer>(&self, s: S) -> core::result::Result<S::Ok, S::Error> {
        s.serialize_u64(self.0)
    }
}
impl<'de> Deserialize<'de> for UnixMillis {
    fn deserialize<D: Deserializer<'de>>(d: D) -> core::result::Result<Self, D::Error> {
        let v = u64::deserialize(d)?;
        Ok(UnixMillis(v))
    }
}

// Pubkey <-> base58 codec for serde, without heap churn on hot paths.
pub mod serde_pubkey {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(k: &Pubkey, s: S) -> core::result::Result<S::Ok, S::Error> {
        s.serialize_str(&k.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> core::result::Result<Pubkey, D::Error> {
        let s = String::deserialize(d)?;
        Pubkey::from_str(&s).map_err(serde::de::Error::custom)
    }
}
