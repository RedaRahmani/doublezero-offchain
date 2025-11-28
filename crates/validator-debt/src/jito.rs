use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, anyhow};
use backon::{ExponentialBuilder, Retryable};
use serde::Deserialize;
use tracing::{debug, info};

use crate::solana_debt_calculator::ValidatorRewards;

const JITO_BASE_URL: &str = "https://kobe.mainnet.jito.network/api/v1/";

// Safety cap to avoid runaway pagination if the API misbehaves.
const MAX_JITO_PAGES: u16 = 25;
pub const JITO_REWARDS_LIMIT: u16 = 1_500;

#[derive(Deserialize, Debug)]
pub struct JitoRewards {
    pub total_count: usize,
    pub rewards: Vec<JitoReward>,
}

#[derive(Deserialize, Debug)]
pub struct JitoReward {
    pub vote_account: String,
    pub mev_revenue: u64,
}

// may need to add in pagination
pub async fn get_jito_rewards<'a>(
    solana_debt_calculator: &impl ValidatorRewards,
    validator_ids: &'a [String],
    epoch: u64,
) -> Result<HashMap<&'a str, u64>> {
    let mut page: u16 = 1;
    let mut total_count: Option<usize> = None;
    let mut collected: usize = 0;
    let mut all_rewards: HashMap<String, u64> = HashMap::new();
    let mut pagination_notice_logged = false;

    let retry_strategy = ExponentialBuilder::default()
        .with_max_times(5)
        .with_min_delay(Duration::from_millis(100))
        .with_max_delay(Duration::from_secs(10))
        .with_jitter();

    loop {
        if page > MAX_JITO_PAGES {
            info!(
                "Stopping Jito rewards pagination for epoch {epoch}: reached MAX_JITO_PAGES={MAX_JITO_PAGES} after collecting {collected} entries; returning partial results"
            );
            break;
        }

        let url = format!(
            "{JITO_BASE_URL}validator_rewards?epoch={epoch}&page={page}&limit={JITO_REWARDS_LIMIT}"
        );
        debug!("Fetching Jito rewards for epoch {epoch}, page {page}");
        let page_rewards = (|| async { solana_debt_calculator.get::<JitoRewards>(&url).await })
            .retry(&retry_strategy)
            .notify(|err, dur: Duration| {
                info!("Jito API call failed, retrying in {:?}: {}", dur, err);
            })
            .await
            .map_err(|e| anyhow!(e))
            .with_context(|| {
                format!("Failed to fetch Jito rewards page {page} for epoch {epoch} after retries")
            })?;

        if total_count.is_none() {
            total_count = Some(page_rewards.total_count);
            let page_size = usize::from(JITO_REWARDS_LIMIT);
            if page_rewards.total_count > page_size && !pagination_notice_logged {
                info!(
                    "Detected paginated Jito rewards for epoch {epoch}: total_count={} exceeds page size {JITO_REWARDS_LIMIT}",
                    page_rewards.total_count,
                );
                pagination_notice_logged = true;
            }
        }

        if page_rewards.rewards.is_empty() {
            info!(
                "Stopping Jito rewards pagination for epoch {epoch}: empty page returned at page {page}"
            );
            break;
        }

        let page_len = page_rewards.rewards.len();
        for reward in page_rewards.rewards {
            *all_rewards.entry(reward.vote_account).or_insert(0) += reward.mev_revenue;
        }
        collected += page_len;

        if let Some(total) = total_count {
            if collected >= total {
                info!(
                    "Collected all Jito rewards for epoch {epoch}: collected {collected} of {total} after page {page}"
                );
                break;
            }
        }

        page += 1;
    }

    let jito_rewards = validator_ids
        .iter()
        .map(|validator_id| {
            let mev_revenue = all_rewards.get(validator_id).copied().unwrap_or_default();
            (validator_id.as_str(), mev_revenue)
        })
        .collect::<HashMap<_, _>>();

    Ok(jito_rewards)
}

#[cfg(test)]
mod tests {
    use mockall::Sequence;

    use super::*;
    use crate::solana_debt_calculator::MockValidatorRewards;

    #[tokio::test]
    async fn get_jito_rewards_handles_single_page_when_total_under_limit() {
        let mut jito_mock_fetcher = MockValidatorRewards::new();
        let validator_a = "ValidatorA";
        let validator_b = "ValidatorB";
        let validator_ids = vec![validator_a.to_string(), validator_b.to_string()];
        let epoch = 812;
        let validator_a_revenue = 42;
        let validator_b_revenue = 84;

        jito_mock_fetcher
            .expect_get::<JitoRewards>()
            .withf(move |url| {
                url.contains(&format!("epoch={epoch}"))
                    && url.contains("page=1")
                    && url.contains(&format!("limit={JITO_REWARDS_LIMIT}"))
            })
            .times(1)
            .return_once(move |_| {
                Ok(JitoRewards {
                    total_count: 2,
                    rewards: vec![
                        JitoReward {
                            vote_account: validator_a.to_string(),
                            mev_revenue: validator_a_revenue,
                        },
                        JitoReward {
                            vote_account: validator_b.to_string(),
                            mev_revenue: validator_b_revenue,
                        },
                    ],
                })
            });

        let mock_response = get_jito_rewards(&jito_mock_fetcher, validator_ids.as_slice(), epoch)
            .await
            .unwrap();

        assert_eq!(mock_response.get(validator_a), Some(&validator_a_revenue));
        assert_eq!(mock_response.get(validator_b), Some(&validator_b_revenue));
    }

    #[tokio::test]
    async fn get_jito_rewards_fetches_all_pages_when_total_exceeds_page_size() {
        let mut jito_mock_fetcher = MockValidatorRewards::new();
        let validator_a = "ValidatorA";
        let validator_b = "ValidatorB";
        let validator_c = "ValidatorC";
        let validator_ids = vec![
            validator_a.to_string(),
            validator_b.to_string(),
            validator_c.to_string(),
        ];
        let epoch = 900;

        let mut seq = Sequence::new();
        jito_mock_fetcher
            .expect_get::<JitoRewards>()
            .withf(move |url| url.contains(&format!("epoch={epoch}")) && url.contains("page=1"))
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |_| {
                Ok(JitoRewards {
                    total_count: 3,
                    rewards: vec![
                        JitoReward {
                            vote_account: validator_a.to_string(),
                            mev_revenue: 10,
                        },
                        JitoReward {
                            vote_account: validator_b.to_string(),
                            mev_revenue: 20,
                        },
                    ],
                })
            });

        jito_mock_fetcher
            .expect_get::<JitoRewards>()
            .withf(move |url| url.contains(&format!("epoch={epoch}")) && url.contains("page=2"))
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |_| {
                Ok(JitoRewards {
                    total_count: 3,
                    rewards: vec![JitoReward {
                        vote_account: validator_c.to_string(),
                        mev_revenue: 30,
                    }],
                })
            });

        let mock_response = get_jito_rewards(&jito_mock_fetcher, validator_ids.as_slice(), epoch)
            .await
            .unwrap();

        assert_eq!(mock_response.get(validator_a), Some(&10));
        assert_eq!(mock_response.get(validator_b), Some(&20));
        assert_eq!(mock_response.get(validator_c), Some(&30));
    }

    #[tokio::test]
    async fn get_jito_rewards_stops_on_empty_page() {
        let mut jito_mock_fetcher = MockValidatorRewards::new();
        let validator_a = "ValidatorA";
        let validator_b = "ValidatorB";
        let validator_c = "ValidatorC";
        let validator_ids = vec![
            validator_a.to_string(),
            validator_b.to_string(),
            validator_c.to_string(),
        ];
        let epoch = 901;

        let mut seq = Sequence::new();
        jito_mock_fetcher
            .expect_get::<JitoRewards>()
            .withf(move |url| url.contains(&format!("epoch={epoch}")) && url.contains("page=1"))
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |_| {
                Ok(JitoRewards {
                    total_count: 4,
                    rewards: vec![
                        JitoReward {
                            vote_account: validator_a.to_string(),
                            mev_revenue: 5,
                        },
                        JitoReward {
                            vote_account: validator_b.to_string(),
                            mev_revenue: 15,
                        },
                    ],
                })
            });

        jito_mock_fetcher
            .expect_get::<JitoRewards>()
            .withf(move |url| url.contains(&format!("epoch={epoch}")) && url.contains("page=2"))
            .times(1)
            .in_sequence(&mut seq)
            .return_once(move |_| {
                Ok(JitoRewards {
                    total_count: 4,
                    rewards: vec![],
                })
            });

        let mock_response = get_jito_rewards(&jito_mock_fetcher, validator_ids.as_slice(), epoch)
            .await
            .unwrap();

        assert_eq!(mock_response.get(validator_a), Some(&5));
        assert_eq!(mock_response.get(validator_b), Some(&15));
        assert_eq!(mock_response.get(validator_c), Some(&0));
    }
}
