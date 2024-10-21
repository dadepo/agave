#![allow(clippy::arithmetic_side_effects)]
#![feature(test)]
extern crate solana_ledger;
extern crate test;

use {
    bincode::serialize,
    rand::Rng,
    solana_entry::entry::{create_ticks, Entry},
    solana_ledger::{
        blockstore::{
            entries_to_test_shreds, get_last_hash, make_slot_entries_with_transactions, Blockstore,
        },
        blockstore_meta::SlotMeta,
        get_tmp_ledger_path_auto_delete,
    },
    solana_sdk::{
        clock::Slot, hash::Hash, message::v0::LoadedAddresses,
        transaction_context::TransactionReturnData,
    },
    solana_transaction_status::{TransactionStatusMeta, VersionedTransactionWithStatusMeta},
    std::path::Path,
    test::Bencher,
};

// Given some shreds and a ledger at ledger_path, benchmark writing the shreds to the ledger
fn bench_write_shreds(bench: &mut Bencher, entries: Vec<Entry>, ledger_path: &Path) {
    let blockstore =
        Blockstore::open(ledger_path).expect("Expected to be able to open database ledger");
    bench.iter(move || {
        let shreds = entries_to_test_shreds(&entries, 0, 0, true, 0, /*merkle_variant:*/ true);
        blockstore.insert_shreds(shreds, None, false).unwrap();
    });
}

// Insert some shreds into the ledger in preparation for read benchmarks
fn setup_read_bench(
    blockstore: &Blockstore,
    num_small_shreds: u64,
    num_large_shreds: u64,
    slot: Slot,
) {
    // Make some big and small entries
    let entries = create_ticks(
        num_large_shreds * 4 + num_small_shreds * 2,
        0,
        Hash::default(),
    );

    // Convert the entries to shreds, write the shreds to the ledger
    let shreds = entries_to_test_shreds(
        &entries,
        slot,
        slot.saturating_sub(1), // parent_slot
        true,                   // is_full_slot
        0,                      // version
        true,                   // merkle_variant
    );
    blockstore
        .insert_shreds(shreds, None, false)
        .expect("Expectd successful insertion of shreds into ledger");
}

// Write small shreds to the ledger
#[bench]
#[ignore]
fn bench_write_small(bench: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path_auto_delete!();
    let num_entries = 32 * 1024;
    let entries = create_ticks(num_entries, 0, Hash::default());
    bench_write_shreds(bench, entries, ledger_path.path());
}

// Write big shreds to the ledger
#[bench]
#[ignore]
fn bench_write_big(bench: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path_auto_delete!();
    let num_entries = 32 * 1024;
    let entries = create_ticks(num_entries, 0, Hash::default());
    bench_write_shreds(bench, entries, ledger_path.path());
}

#[bench]
#[ignore]
fn bench_read_sequential(bench: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path_auto_delete!();
    let blockstore =
        Blockstore::open(ledger_path.path()).expect("Expected to be able to open database ledger");

    // Insert some big and small shreds into the ledger
    let num_small_shreds = 32 * 1024;
    let num_large_shreds = 32 * 1024;
    let total_shreds = num_small_shreds + num_large_shreds;
    let slot = 0;
    setup_read_bench(&blockstore, num_small_shreds, num_large_shreds, slot);

    let num_reads = total_shreds / 15;
    let mut rng = rand::thread_rng();
    bench.iter(move || {
        // Generate random starting point in the range [0, total_shreds - 1], read num_reads shreds sequentially
        let start_index = rng.gen_range(0..num_small_shreds + num_large_shreds);
        for i in start_index..start_index + num_reads {
            let _ = blockstore.get_data_shred(slot, i % total_shreds);
        }
    });
}

#[bench]
#[ignore]
fn bench_read_random(bench: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path_auto_delete!();
    let blockstore =
        Blockstore::open(ledger_path.path()).expect("Expected to be able to open database ledger");

    // Insert some big and small shreds into the ledger
    let num_small_shreds = 32 * 1024;
    let num_large_shreds = 32 * 1024;
    let total_shreds = num_small_shreds + num_large_shreds;
    let slot = 0;
    setup_read_bench(&blockstore, num_small_shreds, num_large_shreds, slot);

    let num_reads = total_shreds / 15;

    // Generate a num_reads sized random sample of indexes in range [0, total_shreds - 1],
    // simulating random reads
    let mut rng = rand::thread_rng();
    let indexes: Vec<usize> = (0..num_reads)
        .map(|_| rng.gen_range(0..total_shreds) as usize)
        .collect();
    bench.iter(move || {
        for i in indexes.iter() {
            let _ = blockstore.get_data_shred(slot, *i as u64);
        }
    });
}

#[bench]
#[ignore]
fn bench_insert_data_shred_small(bench: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path_auto_delete!();
    let blockstore =
        Blockstore::open(ledger_path.path()).expect("Expected to be able to open database ledger");
    let num_entries = 32 * 1024;
    let entries = create_ticks(num_entries, 0, Hash::default());
    bench.iter(move || {
        let shreds = entries_to_test_shreds(&entries, 0, 0, true, 0, /*merkle_variant:*/ true);
        blockstore.insert_shreds(shreds, None, false).unwrap();
    });
}

#[bench]
#[ignore]
fn bench_insert_data_shred_big(bench: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path_auto_delete!();
    let blockstore =
        Blockstore::open(ledger_path.path()).expect("Expected to be able to open database ledger");
    let num_entries = 32 * 1024;
    let entries = create_ticks(num_entries, 0, Hash::default());
    bench.iter(move || {
        let shreds = entries_to_test_shreds(&entries, 0, 0, true, 0, /*merkle_variant:*/ true);
        blockstore.insert_shreds(shreds, None, false).unwrap();
    });
}

#[bench]
fn bench_get_complete_block(bench: &mut Bencher) {
    let slot = 10;
    let entries = make_slot_entries_with_transactions(100);
    let shreds = entries_to_test_shreds(
        &entries,
        slot,
        slot - 1, // parent_slot
        true,     // is_full_slot
        0,        // version
        true,     // merkle_variant
    );
    let more_shreds = entries_to_test_shreds(
        &entries,
        slot + 1,
        slot, // parent_slot
        true, // is_full_slot
        0,    // version
        true, // merkle_variant
    );
    let unrooted_shreds = entries_to_test_shreds(
        &entries,
        slot + 2,
        slot + 1, // parent_slot
        true,     // is_full_slot
        0,        // version
        true,     // merkle_variant
    );
    let ledger_path = get_tmp_ledger_path_auto_delete!();
    let blockstore = Blockstore::open(ledger_path.path()).unwrap();

    blockstore.insert_shreds(shreds, None, false).unwrap();
    blockstore.insert_shreds(more_shreds, None, false).unwrap();
    blockstore
        .insert_shreds(unrooted_shreds, None, false)
        .unwrap();
    blockstore
        .set_roots([slot - 1, slot, slot + 1].iter())
        .unwrap();

    let parent_meta = SlotMeta::default();
    blockstore
        .put_meta_bytes(slot - 1, &serialize(&parent_meta).unwrap())
        .unwrap();

    let expected_transactions: Vec<VersionedTransactionWithStatusMeta> = entries
        .iter()
        .filter(|entry| !entry.is_tick())
        .cloned()
        .flat_map(|entry| entry.transactions)
        .map(|transaction| {
            let mut pre_balances: Vec<u64> = vec![];
            let mut post_balances: Vec<u64> = vec![];
            for i in 0..transaction.message.static_account_keys().len() {
                pre_balances.push(i as u64 * 10);
                post_balances.push(i as u64 * 11);
            }
            let compute_units_consumed = Some(12345);
            let signature = transaction.signatures[0];
            let status = TransactionStatusMeta {
                status: Ok(()),
                fee: 42,
                pre_balances: pre_balances.clone(),
                post_balances: post_balances.clone(),
                inner_instructions: Some(vec![]),
                log_messages: Some(vec![]),
                pre_token_balances: Some(vec![]),
                post_token_balances: Some(vec![]),
                rewards: Some(vec![]),
                loaded_addresses: LoadedAddresses::default(),
                return_data: Some(TransactionReturnData::default()),
                compute_units_consumed,
            }
                .into();
            blockstore
                .transaction_status_cf
                .put_protobuf((signature, slot), &status)
                .unwrap();
            let status = TransactionStatusMeta {
                status: Ok(()),
                fee: 42,
                pre_balances: pre_balances.clone(),
                post_balances: post_balances.clone(),
                inner_instructions: Some(vec![]),
                log_messages: Some(vec![]),
                pre_token_balances: Some(vec![]),
                post_token_balances: Some(vec![]),
                rewards: Some(vec![]),
                loaded_addresses: LoadedAddresses::default(),
                return_data: Some(TransactionReturnData::default()),
                compute_units_consumed,
            }
                .into();
            blockstore
                .transaction_status_cf
                .put_protobuf((signature, slot + 1), &status)
                .unwrap();
            let status = TransactionStatusMeta {
                status: Ok(()),
                fee: 42,
                pre_balances: pre_balances.clone(),
                post_balances: post_balances.clone(),
                inner_instructions: Some(vec![]),
                log_messages: Some(vec![]),
                pre_token_balances: Some(vec![]),
                post_token_balances: Some(vec![]),
                rewards: Some(vec![]),
                loaded_addresses: LoadedAddresses::default(),
                return_data: Some(TransactionReturnData::default()),
                compute_units_consumed,
            }
                .into();
            blockstore
                .transaction_status_cf
                .put_protobuf((signature, slot + 2), &status)
                .unwrap();
            VersionedTransactionWithStatusMeta {
                transaction,
                meta: TransactionStatusMeta {
                    status: Ok(()),
                    fee: 42,
                    pre_balances,
                    post_balances,
                    inner_instructions: Some(vec![]),
                    log_messages: Some(vec![]),
                    pre_token_balances: Some(vec![]),
                    post_token_balances: Some(vec![]),
                    rewards: Some(vec![]),
                    loaded_addresses: LoadedAddresses::default(),
                    return_data: Some(TransactionReturnData::default()),
                    compute_units_consumed,
                },
            }
        })
        .collect();

    bench.iter(move || {
        blockstore.get_complete_block(slot + 2, true).unwrap();
    });
}