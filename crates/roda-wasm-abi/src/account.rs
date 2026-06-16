//! Safe wrappers over the balance / account / flag host verbs (ADR-014, ADR-022).
//!
//! Mirrors the host's signedness conventions: amounts are unsigned `u64`,
//! balances signed `i64`, flag lanes a single byte (0..=7). `credit` moves
//! value out of an account, `debit` moves it in.

use crate::ffi;

/// Move `amount` out of `account` (decreases its balance).
pub fn credit(account: u64, amount: u64) {
    unsafe { ffi::credit(account, amount) }
}

/// Move `amount` into `account` (increases its balance).
pub fn debit(account: u64, amount: u64) {
    unsafe { ffi::debit(account, amount) }
}

/// Read the current signed balance of `account`.
pub fn balance(account: u64) -> i64 {
    unsafe { ffi::get_balance(account) }
}

/// Get-or-create the bucket linked to `account` under `type_id` (ADR-022 §6);
/// returns the child account id.
pub fn linked_account(account: u64, type_id: u16) -> u64 {
    unsafe { ffi::linked_account(account, type_id as u32) }
}

/// Read flag byte at `lane` (0..=7) of `account`.
pub fn get_flag(account: u64, lane: u8) -> u8 {
    unsafe { ffi::get_flag(account, lane as u32) as u8 }
}

/// Test whether `account`'s `lane` byte equals `value`.
pub fn has_flag(account: u64, lane: u8, value: u8) -> bool {
    unsafe { ffi::has_flag(account, lane as u32, value as u32) != 0 }
}

/// Set `account`'s `lane` byte to `value` (lane 0 is the status lane, ADR-022 §6).
pub fn set_flag(account: u64, lane: u8, value: u8) {
    unsafe { ffi::set_flag(account, lane as u32, value as u32) }
}
