#![allow(clippy::arithmetic_side_effects)]

extern crate solana_ledger;

use std::io::Write;
use {
    rand::Rng,
    solana_entry::entry::{create_ticks, Entry},
    solana_ledger::{
        blockstore::{entries_to_test_shreds, Blockstore},
        get_tmp_ledger_path_auto_delete,
    },
    solana_sdk::{clock::Slot, hash::Hash, pubkey::Pubkey, signature::Signature},
    solana_transaction_status::TransactionStatusMeta,
    std::path::Path,
};
use solana_ledger::blockstore::entries_to_test_code_shreds;
use solana_ledger::shred::Shred;

#[test]
fn bench_read_random() {
    let ledger_path = get_tmp_ledger_path_auto_delete!();
    let blockstore =
        Blockstore::open(ledger_path.path()).expect("Expected to be able to open database ledger");

    // Insert some big and small shreds into the ledger
    let num_small_shreds = 32 * 1024;
    let num_large_shreds = 32 * 1024;
    let slot = 0;

    // Make some big and small entries
    let entries = create_ticks(
        num_large_shreds * 4 + num_small_shreds * 2,
        0,
        Hash::default(),
    );

    // Convert the entries to shreds, write the shreds to the ledger
    let shreds = entries_to_test_code_shreds(
        &entries,
        slot,
        slot.saturating_sub(1), // parent_slot
        true,                   // is_full_slot
        0,                      // version
        true,                   // merkle_variant
    );

    fn save_shreds_to_file(shreds: &[Shred], path: &str) {
        let mut file = std::fs::File::create(path).unwrap();
        for shred in shreds {
            println!("++++++");
            println!("slot = {} index = {}, type = {:?}", shred.slot(), shred.index(), shred.shred_type());
            let payload = shred.payload();
            file.write(&payload.len().to_le_bytes()).unwrap();
            file.write(payload).unwrap();
        }
    }
    save_shreds_to_file(&shreds, "/tmp/shreds/code.bin");

    blockstore
        .insert_shreds(shreds, None, false)
        .expect("Expectd successful insertion of shreds into ledger");

}


