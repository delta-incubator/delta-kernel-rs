//! Snapshot and metadata replay memory probe. Allocator selection is fixed at build time through
//! TRACKING_BENCH_ALLOCATOR=system|process|context; context is the default.

use std::alloc::{GlobalAlloc, Layout, System};
use std::error::Error;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::Snapshot;
use delta_kernel_default_engine::executor::tokio::TokioBackgroundExecutor;
use delta_kernel_default_engine::DefaultEngineBuilder;
use delta_kernel_memory_tracking::{Context, TrackingAllocator};
use peak_alloc::PeakAlloc;
use url::Url;

struct ProbeAllocator;

const MODE: &str = match option_env!("TRACKING_BENCH_ALLOCATOR") {
    Some(mode) => mode,
    None => "context",
};

fn allocator() -> &'static dyn GlobalAlloc {
    match MODE {
        "system" => &System,
        "process" => &PeakAlloc,
        _ => &TrackingAllocator,
    }
}

unsafe impl GlobalAlloc for ProbeAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        unsafe { allocator().alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        unsafe { allocator().alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { allocator().dealloc(pointer, layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        unsafe { allocator().realloc(pointer, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: ProbeAllocator = ProbeAllocator;

fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = std::env::args().skip(1);
    let table = arguments
        .next()
        .ok_or("expected table URL and optional repetition count")?;
    let repeats: usize = arguments.next().unwrap_or_else(|| "10".into()).parse()?;
    if Url::parse(&table)?.scheme() != "file" {
        return Err("this probe requires a local file URL".into());
    }
    let executor = Arc::new(TokioBackgroundExecutor::new());
    let engine = DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new()))
        .with_task_executor(executor)
        .build();
    let mut rows = Vec::new();
    for repeat in 0..=repeats {
        let context = Context::new(repeat as u64 + 1, None);
        let start = Instant::now();
        let scope = context.enter();
        let snapshot = Snapshot::builder_for(&table).build(&engine)?;
        let scan = snapshot.clone().scan_builder().build()?;
        for batch in scan.scan_metadata(&engine)? {
            black_box(batch?);
        }
        let measured = context.snapshot().total;
        drop(scan);
        drop(snapshot);
        drop(scope);
        let elapsed = start.elapsed().as_nanos();
        let retained = context.snapshot().total.live_bytes;
        if repeat > 0 {
            rows.push((repeat, elapsed, measured, retained, context));
        }
    }
    drop(engine);
    println!("mode,repeat,elapsed_ns,live_after_scan,peak_bytes,allocated_bytes,allocations,live_after_drop,tracking_bytes,retained_after_shutdown,final_peak");
    for (repeat, elapsed, measured, retained, context) in rows {
        let final_stats = context.snapshot().total;
        println!(
            "{MODE},{repeat},{elapsed},{},{},{},{},{retained},{},{},{}",
            measured.live_bytes,
            measured.peak_bytes,
            measured.allocated_bytes,
            measured.allocations,
            measured.tracking_bytes,
            final_stats.live_bytes,
            final_stats.peak_bytes
        );
    }
    Ok(())
}
