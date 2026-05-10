//! `GetTerms` RPC — unit-ish tests hitting the `LedgerHandler` directly
//! over a Bare-mode harness. Seeds `term.log` and `vote.log` via the
//! cluster `Term`/`Vote` wrappers and verifies the merge, filter, and
//! pagination behaviour of `get_terms`.

use ::proto::ledger as proto;
use ::proto::ledger::ledger_server::Ledger as LedgerSvc;
use cluster::{LedgerHandler, Role, Term, Vote};
use cluster_test_utils::{ClusterTestingConfig, ClusterTestingControl};
use std::sync::Arc;
use tonic::Request;

async fn setup() -> (ClusterTestingControl, Arc<Term>, Arc<Vote>, LedgerHandler) {
    let ctl = ClusterTestingControl::start(ClusterTestingConfig::bare(Role::Leader))
        .await
        .expect("bare start");
    let term = ctl.term(0).expect("term");
    let vote = ctl.vote(0).expect("vote");
    let handler = ctl.ledger_handler(0).expect("ledger_handler");
    (ctl, term, vote, handler)
}

fn term_info(resp: &proto::GetTermsResponse, term: u64) -> &proto::TermInfo {
    resp.terms
        .iter()
        .find(|t| t.term == term)
        .unwrap_or_else(|| panic!("term {} missing from response", term))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_terms_merges_term_and_vote_logs() {
    let (_ctl, term, vote, handler) = setup().await;

    // Seed: term 1 with start_tx_id 100, voted_for 7
    //       term 2 with start_tx_id 200, voted_for 9 (we voted for the leader)
    //       term 3 vote-only (failed election or ongoing) — voted_for 7
    assert!(term.commit_term(1, 100).expect("commit 1"));
    assert!(vote.vote(1, 7).expect("vote 1"));

    assert!(term.commit_term(2, 200).expect("commit 2"));
    assert!(vote.vote(2, 9).expect("vote 2"));

    assert!(vote.vote(3, 7).expect("vote 3"));

    let resp = handler
        .get_terms(Request::new(proto::GetTermsRequest::default()))
        .await
        .expect("get_terms")
        .into_inner();

    let t1 = term_info(&resp, 1);
    assert_eq!(t1.start_tx_id, 100);
    assert_eq!(t1.voted_for, 7);
    assert!(t1.has_term_record);
    assert!(t1.has_vote_record);

    let t2 = term_info(&resp, 2);
    assert_eq!(t2.start_tx_id, 200);
    assert_eq!(t2.voted_for, 9);
    assert!(t2.has_term_record);
    assert!(t2.has_vote_record);

    let t3 = term_info(&resp, 3);
    assert_eq!(t3.start_tx_id, 0);
    assert_eq!(t3.voted_for, 7);
    assert!(!t3.has_term_record);
    assert!(t3.has_vote_record);

    // Response is ascending by term.
    let returned: Vec<u64> = resp.terms.iter().map(|t| t.term).collect();
    let mut sorted = returned.clone();
    sorted.sort();
    assert_eq!(returned, sorted);

    // No more pages when we ask for everything.
    assert_eq!(resp.next_term, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_terms_filters_by_from_term() {
    let (_ctl, term, vote, handler) = setup().await;
    assert!(term.commit_term(1, 10).expect("commit 1"));
    assert!(term.commit_term(2, 20).expect("commit 2"));
    assert!(term.commit_term(3, 30).expect("commit 3"));
    assert!(vote.vote(1, 1).expect("vote 1"));
    assert!(vote.vote(2, 2).expect("vote 2"));
    assert!(vote.vote(3, 3).expect("vote 3"));

    let resp = handler
        .get_terms(Request::new(proto::GetTermsRequest {
            from_term: 2,
            limit: 0,
        }))
        .await
        .expect("get_terms")
        .into_inner();

    let returned: Vec<u64> = resp.terms.iter().map(|t| t.term).collect();
    assert_eq!(returned, vec![2, 3]);
    assert_eq!(resp.next_term, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_terms_paginates_with_next_term() {
    let (_ctl, term, vote, handler) = setup().await;
    for t in 1u64..=5 {
        assert!(term.commit_term(t, t * 10).expect("commit"));
        assert!(vote.vote(t, t).expect("vote"));
    }

    let page1 = handler
        .get_terms(Request::new(proto::GetTermsRequest {
            from_term: 0,
            limit: 2,
        }))
        .await
        .expect("get_terms p1")
        .into_inner();

    assert_eq!(page1.terms.len(), 2);
    assert_eq!(page1.terms[0].term, 1);
    assert_eq!(page1.terms[1].term, 2);
    assert_eq!(page1.next_term, 3);

    let page2 = handler
        .get_terms(Request::new(proto::GetTermsRequest {
            from_term: page1.next_term,
            limit: 2,
        }))
        .await
        .expect("get_terms p2")
        .into_inner();

    assert_eq!(page2.terms.len(), 2);
    assert_eq!(page2.terms[0].term, 3);
    assert_eq!(page2.terms[1].term, 4);
    assert_eq!(page2.next_term, 5);

    let page3 = handler
        .get_terms(Request::new(proto::GetTermsRequest {
            from_term: page2.next_term,
            limit: 2,
        }))
        .await
        .expect("get_terms p3")
        .into_inner();

    assert_eq!(page3.terms.len(), 1);
    assert_eq!(page3.terms[0].term, 5);
    assert_eq!(page3.next_term, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_terms_empty_logs_returns_empty() {
    let (_ctl, _term, _vote, handler) = setup().await;
    let resp = handler
        .get_terms(Request::new(proto::GetTermsRequest::default()))
        .await
        .expect("get_terms")
        .into_inner();
    assert!(resp.terms.is_empty());
    assert_eq!(resp.next_term, 0);
}
