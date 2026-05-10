use ::proto::ledger::WaitLevel;
use ::proto::ledger::wal_log_record::Entry as LogEntry;
use client::NodeClient;
use cluster_test_utils::{ClusterTestingConfig, ClusterTestingControl};

async fn setup() -> (ClusterTestingControl, NodeClient) {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::standalone())
        .await
        .expect("standalone start");
    let client = ctl.raw_client_for_slot(0).expect("raw client").clone();
    (ctl, client)
}

fn count_metadata(records: &[::proto::ledger::WalLogRecord]) -> usize {
    records
        .iter()
        .filter(|r| matches!(r.entry, Some(LogEntry::Metadata(_))))
        .count()
}

fn metadata_tx_ids(records: &[::proto::ledger::WalLogRecord]) -> Vec<u64> {
    records
        .iter()
        .filter_map(|r| match &r.entry {
            Some(LogEntry::Metadata(m)) => Some(m.tx_id),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_log_returns_records_for_full_range() {
    let (_ctl, client) = setup().await;

    for i in 1..=5u64 {
        client
            .deposit_and_wait(1, 100 * i, 0, WaitLevel::Snapshot)
            .await
            .unwrap();
    }

    let page = client.get_log(1, 5, 100).await.unwrap();
    let metas = metadata_tx_ids(&page.records);
    assert_eq!(metas, vec![1, 2, 3, 4, 5], "expected metadata for tx 1..=5");
    assert_eq!(page.next_tx_id, 0, "fully drained range -> next_tx_id=0");
    assert!(page.last_commit_tx_id >= 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_log_filters_by_to_tx_id() {
    let (_ctl, client) = setup().await;

    for _ in 0..5 {
        client
            .deposit_and_wait(1, 100, 0, WaitLevel::Snapshot)
            .await
            .unwrap();
    }

    let page = client.get_log(3, 4, 100).await.unwrap();
    assert_eq!(metadata_tx_ids(&page.records), vec![3, 4]);
    assert_eq!(page.next_tx_id, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_log_from_zero_includes_structural_records() {
    let (_ctl, client) = setup().await;

    client
        .deposit_and_wait(1, 100, 0, WaitLevel::Snapshot)
        .await
        .unwrap();

    let page = client.get_log(0, 0, 100).await.unwrap();
    let has_segment_header = page
        .records
        .iter()
        .any(|r| matches!(r.entry, Some(LogEntry::SegmentHeader(_))));
    assert!(
        has_segment_header,
        "from_tx_id=0 should include WalSegmentHeader"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_log_paginates_without_duplicates() {
    let (_ctl, client) = setup().await;

    let n: u64 = 20;
    for _ in 0..n {
        client
            .deposit_and_wait(1, 100, 0, WaitLevel::Snapshot)
            .await
            .unwrap();
    }

    let mut seen: Vec<u64> = Vec::new();
    let mut from = 1u64;
    let mut iterations = 0;
    loop {
        iterations += 1;
        assert!(iterations < 100, "pagination loop must terminate");
        let page = client.get_log(from, 0, 5).await.unwrap();
        if page.records.is_empty() {
            break;
        }
        for tx_id in metadata_tx_ids(&page.records) {
            seen.push(tx_id);
        }
        if page.next_tx_id == 0 {
            break;
        }
        assert!(page.next_tx_id > from, "next_tx_id must advance");
        from = page.next_tx_id;
    }

    let expected: Vec<u64> = (1..=n).collect();
    assert_eq!(seen, expected, "all tx ids retrieved exactly once, in order");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_log_rejects_inverted_range() {
    let (_ctl, client) = setup().await;
    let err = client.get_log(10, 5, 100).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_log_caps_oversized_limit() {
    let (_ctl, client) = setup().await;

    for _ in 0..3 {
        client
            .deposit_and_wait(1, 50, 0, WaitLevel::Snapshot)
            .await
            .unwrap();
    }

    let page = client.get_log(1, 0, 50_000).await.unwrap();
    assert_eq!(count_metadata(&page.records), 3);
    assert!(
        page.records.len() <= 10_000,
        "server hard cap of 10,000 records"
    );
}
