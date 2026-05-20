use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::service_metrics::ServiceMetrics;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrekeyBundleStorable {
    pub registration_id: u32,
    pub device_id: u32,
    pub bundle_id: String,
    pub prekey_id: Option<u32>,
    pub prekey_public: Option<Vec<u8>>,
    pub classical_one_time_prekey_present: bool,
    pub classical_one_time_prekey_id: Option<u32>,
    pub signed_prekey_id: u32,
    pub signed_prekey_public: Vec<u8>,
    pub signed_prekey_signature: Vec<u8>,
    pub identity_key_public: Vec<u8>,
    pub kyber_prekey_id: u32,
    pub kyber_prekey_public: Vec<u8>,
    pub kyber_prekey_signature: Vec<u8>,
    pub pq_prekey_id: u32,
    pub pq_prekey_type: String,
    pub pq_prekey_signature_present: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OneTimePrekeyStorable {
    pub prekey_id: u32,
    pub prekey_public: Vec<u8>,
    pub pq_prekey_id: u32,
    pub pq_prekey_public: Vec<u8>,
    pub pq_prekey_signature: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrekeyBundleBatchStorable {
    pub registration_id: u32,
    pub device_id: u32,
    pub signed_prekey_id: u32,
    pub signed_prekey_public: Vec<u8>,
    pub signed_prekey_signature: Vec<u8>,
    pub identity_key_public: Vec<u8>,
    pub last_resort_pq_prekey_id: u32,
    pub last_resort_pq_prekey_public: Vec<u8>,
    pub last_resort_pq_prekey_signature: Vec<u8>,
    pub one_time_prekeys: Vec<OneTimePrekeyStorable>,
    pub signed_prekey_fallback: bool,
}

impl PrekeyBundleBatchStorable {
    fn bundle_id(
        device_id: u32,
        signed_prekey_id: u32,
        classical_prekey_id: Option<u32>,
        pq_prekey_id: u32,
        pq_type: &str,
    ) -> String {
        format!(
            "device{}-spk{}-classical{}-pq{}-{}",
            device_id,
            signed_prekey_id,
            classical_prekey_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "none".to_string()),
            pq_prekey_id,
            pq_type
        )
    }

    fn expand(self) -> Vec<PrekeyBundleStorable> {
        let mut bundles = Vec::with_capacity(
            self.one_time_prekeys.len() + usize::from(self.signed_prekey_fallback),
        );
        let fallback_bundle_id = self.signed_prekey_fallback.then(|| {
            Self::bundle_id(
                self.device_id,
                self.signed_prekey_id,
                None,
                self.last_resort_pq_prekey_id,
                "last_resort",
            )
        });

        for prekey in self.one_time_prekeys {
            let bundle_id = Self::bundle_id(
                self.device_id,
                self.signed_prekey_id,
                Some(prekey.prekey_id),
                prekey.pq_prekey_id,
                "one_time",
            );
            bundles.push(PrekeyBundleStorable {
                registration_id: self.registration_id,
                device_id: self.device_id,
                bundle_id,
                prekey_id: Some(prekey.prekey_id),
                prekey_public: Some(prekey.prekey_public),
                classical_one_time_prekey_present: true,
                classical_one_time_prekey_id: Some(prekey.prekey_id),
                signed_prekey_id: self.signed_prekey_id,
                signed_prekey_public: self.signed_prekey_public.clone(),
                signed_prekey_signature: self.signed_prekey_signature.clone(),
                identity_key_public: self.identity_key_public.clone(),
                kyber_prekey_id: prekey.pq_prekey_id,
                kyber_prekey_public: prekey.pq_prekey_public,
                kyber_prekey_signature: prekey.pq_prekey_signature.clone(),
                pq_prekey_id: prekey.pq_prekey_id,
                pq_prekey_type: "one_time".to_string(),
                pq_prekey_signature_present: !prekey.pq_prekey_signature.is_empty(),
            });
        }

        if let Some(bundle_id) = fallback_bundle_id {
            bundles.push(PrekeyBundleStorable {
                registration_id: self.registration_id,
                device_id: self.device_id,
                bundle_id,
                prekey_id: None,
                prekey_public: None,
                classical_one_time_prekey_present: false,
                classical_one_time_prekey_id: None,
                signed_prekey_id: self.signed_prekey_id,
                signed_prekey_public: self.signed_prekey_public,
                signed_prekey_signature: self.signed_prekey_signature,
                identity_key_public: self.identity_key_public,
                kyber_prekey_id: self.last_resort_pq_prekey_id,
                kyber_prekey_public: self.last_resort_pq_prekey_public,
                kyber_prekey_signature: self.last_resort_pq_prekey_signature.clone(),
                pq_prekey_id: self.last_resort_pq_prekey_id,
                pq_prekey_type: "last_resort".to_string(),
                pq_prekey_signature_present: !self.last_resort_pq_prekey_signature.is_empty(),
            });
        }

        bundles
    }
}

pub struct KeyRepository {
    prekey_bundles: Mutex<HashMap<String, VecDeque<PrekeyBundleStorable>>>,
    one_time_prekey_consumption: Mutex<HashMap<String, u64>>,
    metrics: ServiceMetrics,
}

impl KeyRepository {
    pub fn new() -> Self {
        Self {
            prekey_bundles: Mutex::new(HashMap::new()),
            one_time_prekey_consumption: Mutex::new(HashMap::new()),
            metrics: ServiceMetrics::new(),
        }
    }

    pub fn metrics(&self) -> &ServiceMetrics {
        &self.metrics
    }

    pub fn publish_prekey_bundle(&self, participant: &str, bundle: PrekeyBundleStorable) {
        self.prekey_bundles
            .lock()
            .unwrap()
            .entry(participant.to_string())
            .or_default()
            .push_back(bundle);
    }

    pub fn publish_prekey_bundles(&self, participant: &str, bundles: Vec<PrekeyBundleStorable>) {
        let mut guard = self.prekey_bundles.lock().unwrap();
        let queue = guard.entry(participant.to_string()).or_default();
        queue.extend(bundles);
    }

    pub fn publish_prekey_bundle_batch(&self, participant: &str, batch: PrekeyBundleBatchStorable) {
        self.publish_prekey_bundles(participant, batch.expand());
    }

    pub fn fetch_prekey_bundle(&self, participant: &str) -> Option<PrekeyBundleStorable> {
        self.prekey_bundles
            .lock()
            .unwrap()
            .get(participant)
            .and_then(|queue| queue.front().cloned())
    }

    pub fn consume_one_time_prekey(&self, participant: &str) -> bool {
        let consumed = {
            let mut bundles = self.prekey_bundles.lock().unwrap();
            let Some(queue) = bundles.get_mut(participant) else {
                return false;
            };
            let Some(front) = queue.front() else {
                return false;
            };
            let consumed =
                front.classical_one_time_prekey_present || front.pq_prekey_type == "one_time";
            if consumed {
                queue.pop_front();
            }
            consumed
        };

        if !consumed {
            return false;
        }

        let mut consumption = self.one_time_prekey_consumption.lock().unwrap();
        *consumption.entry(participant.to_string()).or_insert(0) += 1;
        true
    }

    pub fn one_time_prekeys_consumed(&self, participant: &str) -> u64 {
        self.one_time_prekey_consumption
            .lock()
            .unwrap()
            .get(participant)
            .copied()
            .unwrap_or(0)
    }

    pub fn remove_participant(&self, participant: &str) {
        self.prekey_bundles.lock().unwrap().remove(participant);
        self.one_time_prekey_consumption
            .lock()
            .unwrap()
            .remove(participant);
    }
}

impl Default for KeyRepository {
    fn default() -> Self {
        Self::new()
    }
}
