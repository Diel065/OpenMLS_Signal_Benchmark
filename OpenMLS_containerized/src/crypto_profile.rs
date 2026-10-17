use std::{fmt, str::FromStr};

use openmls::prelude::Ciphersuite;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CryptoProfile {
    #[default]
    Classical,
    HybridPq,
}

impl CryptoProfile {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Classical => "classical",
            Self::HybridPq => "hybrid-pq",
        }
    }

    pub const fn ciphersuite(self) -> Ciphersuite {
        match self {
            Self::Classical => Ciphersuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519,
            Self::HybridPq => Ciphersuite::MLS_128_MLKEM768X25519_AES128GCM_SHA256_Ed25519,
        }
    }
}

impl FromStr for CryptoProfile {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "classical" => Ok(Self::Classical),
            "hybrid-pq" => Ok(Self::HybridPq),
            _ => anyhow::bail!("Invalid crypto profile '{value}'; expected classical or hybrid-pq"),
        }
    }
}

impl fmt::Display for CryptoProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}
