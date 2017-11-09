
use rocksdb::{TableFilter, TableProperties, UserCollectedProperties};
use super::properties::PROP_MAX_TS;

struct MaxTsFilter{
    ts: u64,
}

impl MaxTsFilter {
    pub fn new(ts: u64) -> MaxTsFilter {
        MaxTsFilter {
            ts: ts,
        }
    }
}

impl TableFilter for MaxTsFilter {
    fn table_filter(&self, props: &TableProperties) -> bool {
        let user_props = props.user_collected_properties();
        if let Some(encoded) = user_props.get(PROP_MAX_TS) {
            if encoded.len() == 8 {
                let ts = encoded.decode_u64().unwrap();
                // skip sst whose max_ts is less than self.ts
                if ts < self.ts {
                    return false;
                }
            } else {
                panic!("invalid max ts properties");
            }
        }

        true
    }
}
